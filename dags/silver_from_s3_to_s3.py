import logging

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from utils.datasets import RAW_DATASET_CIAN_FLATS, SILVER_DATASET_CIAN_FLATS
from utils.duckdb import get_duckdb_s3_connection

OWNER = "ilyas"
DAG_ID = "silver_from_s3_to_s3"

LAYER_SOURCE = "raw"
LAYER_TARGET = "silver"

SHORT_DESCRIPTION = "Трансформация, очистка и типизация данных из JSONL в Parquet с применением бизнес-логики."

LONG_DESCRIPTION = """
## DAG: Silver Layer Transformation
Процесс преобразует грязные сырые данные в структурированный формат **Parquet**.

### Логика трансформации (DuckDB SQL):
- **Типизация**: Приведение цен к `BIGINT`, площади к `NUMERIC`, дат к `TIMESTAMP` и т.д.
- **Парсинг Title**: Извлечение площади, этажности, комнатности и типа жилья через регулярки.
- **Гео-данные**: Разбор адреса на город, округ и район. Для новой москвы район может быть null
- **Транспорт**: Категоризация доступности метро (пешком/транспорт) и расчет времени до метро.
- **Очистка**:
    - Удаление дублей по бизнес-ключу (нормализованный адрес + этаж + площадь + комнатность).
    - Удаление фейковых объявлений (слишком дешевые или с битыми данными).

### DQ проверки:
- Контроль объема удаленных данных.
- Мониторинг аномалий (слишком маленькая или большая площадь, цена).
- Количество уникальных районов или округов (слишком много может указывать на проблемы с парсингом адресов).
"""


default_args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2026, 1, 18, tz="Europe/Moscow"),
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=10),
}


def get_and_transform_raw_data_to_silver_s3(**context) -> dict[str, int]:
    """Очистка, дедубликация данных из слоя raw в silver .parquet и сохранение в S3"""
    dt = context["data_interval_end"].in_timezone("Europe/Moscow")
    raw_s3_key = (
        f"s3://{LAYER_SOURCE}/cian/year={dt.year}/month={dt.strftime('%m')}/day={dt.strftime('%d')}/flats.jsonl"
    )
    silver_s3_key = (
        f"s3://{LAYER_TARGET}/cian/year={dt.year}/month={dt.strftime('%m')}/day={dt.strftime('%d')}/flats.parquet"
    )

    con = get_duckdb_s3_connection("s3_conn")

    raw_to_silver_query = f"""
        COPY(
        WITH raw_transformed AS (
            SELECT
                id::BIGINT as id,
                -- укорачиваем ссылку
                SPLIT_PART(link, '?', 1)::TEXT as link,
                title::VARCHAR as title,
                -- тип жилья
                CASE
                    WHEN title ILIKE '%апартаменты%' THEN TRUE
                    ELSE FALSE
                END as is_apartament,
                CASE
                    WHEN title ILIKE '%студия%' THEN TRUE
                    ELSE FALSE
                END as is_studio,
                -- площадь из заголовка, число перед м², запятую на точку поменяем
                replace(
                    regexp_replace(
                        NULLIF(regexp_extract(title, '([\d\s]+[.,]?\d*)\s*м²', 1), ''),
                        '\s+', '', 'g'
                    ), 
                    ',', '.'
                )::NUMERIC(10, 2) AS area,
                -- комнатность (0 для студий)
                CASE 
                    WHEN title ILIKE '%студия%' THEN 0
                    ELSE NULLIF(regexp_extract(title, '^(\d+)', 1), '')::INT
                END as rooms_count,
                -- этажи
                NULLIF(regexp_extract(title, '(\d+)/\d+\s*этаж', 1), '')::INT as floor,
                NULLIF(regexp_extract(title, '\d+/(\d+)\s*этаж', 1), '')::INT as total_floors,
                -- цена, убираем валюту и пробелы 
                regexp_replace(price, '[^0-9]', '', 'g')::BIGINT as price,
                address::TEXT as address,
                -- разбиваем адрес
                SPLIT_PART(address, ',', 1)::VARCHAR as city,
                -- округ только заглавными
                NULLIF(regexp_extract(address, '([А-Яа-я]+АО)', 1), '')::VARCHAR as okrug,
                -- район, для новой москвы null, слишком нестабильно 
                CASE
                    WHEN okrug IN ('НАО', 'ТАО') THEN NULL
                    ELSE NULLIF(regexp_extract(address, '(р-н\s?[^,]+)', 1), '')::VARCHAR
                END as district,
                CASE
                    WHEN okrug IN ('НАО', 'ТАО') THEN TRUE
                    ELSE FALSE
                END as is_new_moscow,
                -- вся инфа о метро
                NULLIF(regexp_extract(metro, '^(.*?)\d+\s+минут', 1), '')::VARCHAR as metro_name,
                NULLIF(regexp_extract(metro, '(\d+)\s+минут', 1), '')::INT as metro_min,
                CASE
                    WHEN metro LIKE '%пешком%' THEN 'walk'
                    WHEN metro LIKE '%транс%' THEN 'transport'
                END as metro_type,
                -- время и описание
                parsed_at::TIMESTAMP as parsed_at,
                description::TEXT as description,
                -- нормализованный адрес, ток для дедубликации
                lower(regexp_replace(address, '[^а-яА-Я0-9]', '', 'g')) as norm_address
            FROM read_json_auto('{raw_s3_key}')
        ),
        -- дедубликация по бизнес ключу (чистый адрес, этажи, кол-во комнат)
        deduplicated AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY norm_address, floor, total_floors, rooms_count
                    ORDER BY parsed_at DESC
                ) as row_num
            FROM raw_transformed
            WHERE area IS NOT NULL -- выкидываем строки с битыми данными
                AND price IS NOT NULL
                AND okrug IS NOT NULL
                AND rooms_count IS NOT NULL
                AND (district IS NOT NULL OR is_new_moscow) -- у новой москвы может не быть райнов
                AND round(price / area) > 50000 -- выкидываем фейки (врятли цена за метр хаты меньше 50к)
        )
        -- сохраняем в parquet, EXLUDE убирает ненужные колонки
        SELECT * EXCLUDE (row_num, norm_address)
        FROM deduplicated
        WHERE row_num = 1) TO '{silver_s3_key}' (FORMAT PARQUET, OVERWRITE TRUE);
    """

    try:
        logging.info(f"💻 Выполняю трансформацию: {raw_s3_key}")
        con.execute(raw_to_silver_query)

    finally:
        con.close()

    logging.info(f"✅ Файл успешно сохранен: {silver_s3_key}")

    return {  # автопуш ключей в xcoms
        "raw_s3_key": raw_s3_key,
        "silver_s3_key": silver_s3_key,
    }


def check_silver_data_quality(**context):
    """Проверка качества данных в silver слое после трансформации"""
    # вытаскиваем словарик ключей из xcoms
    keys = context["ti"].xcom_pull(task_ids="transform_to_silver")
    raw_s3_key = keys["raw_s3_key"]
    silver_s3_key = keys["silver_s3_key"]

    con = get_duckdb_s3_connection("s3_conn")

    try:
        logging.info("💻 Выполняю проверку данных")

        dq_stats: tuple[int, int, float, float, int, int] = con.execute(
            f"""
                SELECT
                    COUNT(*) as total_rows,
                    COUNT(distinct district) as all_districts,
                    COUNT(distinct okrug) as okrugs,
                    MIN(area) as min_area,
                    MAX(area) as max_area,
                    MIN(price) as min_price
                FROM read_parquet('{silver_s3_key}')
            """
        ).fetchone()

        raw_total_rows: int = con.execute(f"SELECT count(*) FROM read_json_auto('{raw_s3_key}')").fetchone()[0]
    finally:
        con.close()

    silver_total_rows, districts, okrugs, min_area, max_area, min_price = dq_stats
    diff: int = raw_total_rows - silver_total_rows  # сколько строк удалилось в процессе трансформации
    percent_removed: float = (diff / raw_total_rows) * 100
    # проверки
    if silver_total_rows == 0:
        raise AirflowFailException("Файл пустой!")

    if percent_removed > 50:
        logging.error(f"❌ Удалено {percent_removed:.2f}% данных после трансформации.")
        raise AirflowFailException("Слишком много данных удалено!")

    if okrugs > 12:
        logging.error(f"❌ Слишком много уникальных округов - {okrugs}")
        raise AirflowFailException("Округов больше 12!")

    if districts > 125:
        logging.warning(f"⚠️ Много уникальных районов - {districts}")

    if min_area < 5:
        logging.warning(f"⚠️ Слишком маленькая площадь: {min_area} м²")

    if max_area > 1500:
        logging.warning(f"⚠️ Подозрительно большая площадь: {max_area} м²")

    if min_price < 1_000_000:
        logging.warning(f"⚠️ Подозрительно маленькая цена: {min_price} руб.")

    logging.info("✅ Проверка пройдена")
    logging.info(f"Удалено дублей и мусора: {diff} строк ({percent_removed:.2f}%).")

    return {"raw_count": raw_total_rows, "silver_count": silver_total_rows, "removed": diff}


with DAG(
    dag_id=DAG_ID,
    schedule=[RAW_DATASET_CIAN_FLATS],  # как только обновится датасет raw запустится этот DAG
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["s3", "silver"],
    description=SHORT_DESCRIPTION,
    doc_md=LONG_DESCRIPTION,  # будет отображаться в UI Airflow при открытии дага
) as dag:
    start = EmptyOperator(
        task_id="start",
    )

    transform_to_silver = PythonOperator(
        task_id="transform_to_silver",
        python_callable=get_and_transform_raw_data_to_silver_s3,
    )

    check_data_quality = PythonOperator(
        task_id="check_data_quality",
        python_callable=check_silver_data_quality,
    )

    end = EmptyOperator(
        task_id="end",
        outlets=[SILVER_DATASET_CIAN_FLATS],
    )

    start >> transform_to_silver >> check_data_quality >> end
