import logging

import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from utils.datasets import RAW_DATASET_CIAN_FLATS, SILVER_DATASET_CIAN_FLATS
from utils.duckdb import get_duckdb_s3_connection

OWNER = "ilyas"
DAG_ID = "silver_from_s3_to_s3"

LAYER_SOURCE = "raw"
LAYER_TARGET = "silver"

SHORT_DESCRIPTION = (
    "DAG для трансформации данных из слоя raw в слой silver, из jsonl втипизированный parquet и сохранение в S3"
)

default_args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2026, 1, 18, tz="Europe/Moscow"),
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=10),
}


def get_and_transform_raw_data_to_silver_s3(**context) -> dict[str, int]:
    # Формируем путь к файлу в S3
    dt = context["data_interval_start"].in_timezone("Europe/Moscow")
    raw_s3_key = (
        f"s3://{LAYER_SOURCE}/cian/year={dt.year}/month={dt.strftime('%m')}/day={dt.strftime('%d')}/flats.jsonl"
    )
    silver_s3_key = (
        f"s3://{LAYER_TARGET}/cian/year={dt.year}/month={dt.strftime('%m')}/day={dt.strftime('%d')}/flats.parquet"
    )

    con = get_duckdb_s3_connection("s3_conn")

    raw_count: int = con.execute(f"SELECT count(*) FROM read_json_auto('{raw_s3_key}')").fetchone()[0]
    logging.info(f"📊 Входящие данные (raw): {raw_count} строк.")

    logging.info(f"💻 Выполняю трансформацию: {raw_s3_key}")
    # основной ETL
    con.execute(
        f"""
        COPY(
        WITH raw_transformed AS (
            SELECT
                id::BIGINT as id,
                link::TEXT as link,
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
                replace(NULLIF(regexp_extract(title, '(\d+[.,]?\d*)\s*м²', 1), ''), ',', '.')::NUMERIC(10, 2) as area,
                -- комнатность (0 для студий и своб. планировок)
                CASE 
                    WHEN title ILIKE '%студия%' THEN 0
                    WHEN title ILIKE '%своб%' THEN 0
                    ELSE NULLIF(regexp_extract(title, '^(\d+)', 1), '')::INT
                END as rooms_count,
                -- этажи
                NULLIF(regexp_extract(title, '(\d+)/\d+\s*этаж', 1), '')::INT as floor,
                NULLIF(regexp_extract(title, '\d+/(\d+)\s*этаж', 1), '')::INT as total_floors,
                -- цена, убираем валюту и пробелы 
                regexp_replace(price, '[^0-9]', '', 'g')::BIGINT as price,
                address::TEXT as address,
                -- разбиваем адрес
                trim(SPLIT_PART(address, ',', 1))::VARCHAR as city,
                -- округ только заглавными
                NULLIF(regexp_extract(address, '([А-Яа-я]+АО)', 1), '')::VARCHAR as okrug,
                trim(SPLIT_PART(address, ',', 3))::VARCHAR as district,
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
            WHERE area IS NOT NULL -- выкидываем строки с битыми заголовками
                AND price IS NOT NULL
                AND okrug IS NOT NULL
        )
        -- сохраняем в Parquet, убирая не нужные колонки
        SELECT * EXCLUDE (row_num, norm_address)
        FROM deduplicated
        WHERE row_num = 1 ) TO '{silver_s3_key}' (FORMAT PARQUET);
        """
    )

    silver_count: int = con.execute(f"SELECT count(*) FROM read_parquet('{silver_s3_key}')").fetchone()[0]
    logging.info(f"Данные после дедубликации (silver): {silver_count} строк.")

    con.close()

    diff: int = raw_count - silver_count  # сколько строк удалилось в процессе трансформации
    logging.info(f"Удалено дублей и мусора: {diff} строк ({(diff / raw_count) * 100:.2f}%).")
    logging.info(f"✅ Файл успешно сохранен: {silver_s3_key}")
    return {"raw_count": raw_count, "silver_count": silver_count, "removed": diff}


with DAG(
    dag_id=DAG_ID,
    schedule=[RAW_DATASET_CIAN_FLATS],  # как только обновится датасет raw запустится этот DAG
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["s3", "silver"],
    description=SHORT_DESCRIPTION,
) as dag:
    start = EmptyOperator(
        task_id="start",
    )

    transform_to_silver = PythonOperator(
        task_id="transform_to_silver",
        python_callable=get_and_transform_raw_data_to_silver_s3,
    )

    end = EmptyOperator(
        task_id="end",
        outlets=[SILVER_DATASET_CIAN_FLATS],
    )

    start >> transform_to_silver >> end
