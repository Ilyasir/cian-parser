import logging

import pendulum
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.log.secrets_masker import mask_secret
from utils.datasets import GOLD_DATASET_HISTORY, SILVER_DATASET_CIAN_FLATS
from utils.duckdb import get_duckdb_s3_connection
from utils.sql import load_sql

OWNER = "ilyas"
DAG_ID = "gold_from_s3_to_pg"

LAYER_SOURCE = "silver"
LAYER_TARGET = "gold"

SHORT_DESCRIPTION = "Загрузка данных из S3 в Postgres DWH с поддержкой версионности цен по SCD2."

LONG_DESCRIPTION = """
## DAG: Gold Layer Ingestion (Postgres)
Этот DAG завершает основной процесс обработки данных, перенося их из объектного хранилища (S3)
в реляционную бд **PostgreSQL** для последующей аналитики.
Сохраняет историю изменений цен на квартиры с помощью **SCD2**.

### Основные этапы:
1. **load_from_s3_to_pg_stage**: 
    - Использует **DuckDB** как движок для передачи данных.
    - Через расширение `postgres` и с помощью `ATTACH` подключается напрямую к базе.
    - Очищает `stage_flats` и копирует туда данные из Parquet файла из S3.
    - Выполняет приведение типов ENUM: `okrug_name`, `transport_type`.
2. **merge_from_stage_to_history**:
    - Выполняет SQL-скрипт обновления истории изменений цен в таблице `history_flats`.
    - Реализует логику **SCD2**.

### Логика SCD2 (Бизнес-ключ):
Отслеживаем изменения поля `price` для уникального объекта (`flat_hash`).
Если цена изменилась:
- Поле `effective_to` у текущей записи закрывается датой парсинга.
- Поле `is_active` становится `FALSE`.
- Вставляется новая запись с новой ценой, `is_active = TRUE` и `effective_from = parsed_at`.
"""


default_args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2026, 1, 18, tz="Europe/Moscow"),
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=10),
}


def load_silver_data_from_s3_to_pg(**context) -> None:
    """Копипаст данных из S3 в stage таблицу postgres, с помощью duckdb"""
    dt = context["data_interval_end"].in_timezone("Europe/Moscow")
    silver_s3_key = (
        f"s3://{LAYER_SOURCE}/cian/year={dt.year}/month={dt.strftime('%m')}/day={dt.strftime('%d')}/flats.parquet"
    )
    con = get_duckdb_s3_connection("s3_conn")
    # Получаем параметры для подключения к Postgres из Airflow Connections
    pg_conn = BaseHook.get_connection("pg_conn")
    # Маскируем логин и пароль в логах Airflow
    mask_secret(pg_conn.login)
    mask_secret(pg_conn.password)

    try:
        logging.info(f"💻 Загружаю данные из {silver_s3_key} в stage таблицу")
        # создаем секрет для подключения к постгре
        con.execute(
            f"""
            LOAD postgres;
            CREATE SECRET IF NOT EXISTS dwh_postgres (
                TYPE postgres,
                HOST '{pg_conn.host}',
                PORT {pg_conn.port},
                DATABASE '{pg_conn.schema}',
                USER '{pg_conn.login}',
                PASSWORD '{pg_conn.password}'
            );"""
        )

        con.execute(load_sql("silver_to_stage_dwh.sql", silver_s3_key=silver_s3_key))
    finally:
        con.close()
    logging.info("✅ Успешно загружено в stage таблицу")


with DAG(
    dag_id=DAG_ID,
    schedule=[SILVER_DATASET_CIAN_FLATS],
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["pg", "gold"],
    description=SHORT_DESCRIPTION,
    doc_md=LONG_DESCRIPTION,
) as dag:
    start = EmptyOperator(
        task_id="start",
    )

    load_from_s3_to_pg_stage = PythonOperator(
        task_id="load_from_s3_to_pg_stage", python_callable=load_silver_data_from_s3_to_pg
    )

    merge_from_stage_to_history = SQLExecuteQueryOperator(
        task_id="merge_from_stage_to_history",
        conn_id="pg_conn",
        autocommit=False,
        sql=load_sql("stage_to_history_scd2.sql"),
        show_return_value_in_logs=True,
    )

    end = EmptyOperator(
        task_id="end",
        outlets=[GOLD_DATASET_HISTORY],
    )

    start >> load_from_s3_to_pg_stage >> merge_from_stage_to_history >> end
