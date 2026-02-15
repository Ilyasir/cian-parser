import logging

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from utils.datasets import RAW_DATASET_CIAN_FLATS
from utils.duckdb import get_duckdb_s3_connection

OWNER = "ilyas"
DAG_ID = "raw_from_parser_to_s3"

LAYER = "raw"

SHORT_DESCRIPTION = "DAG для запуска парсера и сохранения сырых данных в S3, проверки качества данных с помощью duckdb"

default_args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2026, 1, 18, tz="Europe/Moscow"),
    "retries": 2,
    "retry_delay": pendulum.duration(hours=2),
}


def check_raw_data_quality(**context) -> dict[str, int]:
    """Проверка качества данных в S3 с помощью duckdb"""
    # Формируем путь к файлу в S3
    dt = context["data_interval_start"].in_timezone("Europe/Moscow")
    raw_s3_key = f"s3://{LAYER}/cian/year={dt.year}/month={dt.strftime('%m')}/day={dt.strftime('%d')}/flats.jsonl"

    con = get_duckdb_s3_connection("s3_conn")

    try:
        logging.info("💻 Выполняю проверку данных")
        dq_stats: tuple[int, int, int, int, int] = con.execute(
            f"""
                SELECT
                    COUNT(*) as total_rows,
                    COUNT(DISTINCT id) as unique_ids,
                    COUNT(price) FILTER (WHERE price IS NOT NULL AND price != '') as valid_prices,
                    COUNT(address) FILTER (WHERE address IS NOT NULL AND address != '') as valid_addresses,
                    COUNT(metro) FILTER (WHERE metro IS NOT NULL AND metro != '') as valid_metro
                FROM read_json_auto('{raw_s3_key}')
            """
        ).fetchone()

    finally:
        con.close()

    total_rows, unique_ids, valid_prices, valid_addresses, valid_metro = dq_stats

    if total_rows == 0:
        raise AirflowFailException("Файл пустой!")
    # метрики качества
    unique_ids_rate: float = unique_ids / total_rows
    valid_prices_rate: float = valid_prices / total_rows
    valid_addresses_rate: float = valid_addresses / total_rows
    valid_metro_rate: float = valid_metro / total_rows
    # проверки
    if unique_ids_rate < 0.90:
        logging.error(f"❌ Проверка не пройдена. Уникальных ID - {unique_ids_rate:.2%}")
        raise AirflowFailException("Слишком много дублирующихся ID!")

    if valid_prices_rate < 0.95:
        logging.error(f"❌ Проверка не пройдена. Процент заполненных цен - {valid_prices_rate:.2%}")
        raise AirflowFailException("Cлишком много пустых цен!")

    if valid_addresses_rate < 0.95:
        logging.error(f"❌ Проверка не пройдена. Процент заполненных адресов - {valid_addresses_rate:.2%}")
        raise AirflowFailException("Cлишком много пустых адресов!")

    logging.info(
        f"✅ Проверка пройдена. Всего строк: {total_rows}. "
        f"Процент заполненных адресов: {valid_addresses_rate:.2%}. "
        f"Процент заполненных метро: {valid_metro_rate:.2%}"
    )

    return {
        "total_rows": total_rows,
        "unique_ids": unique_ids,
        "valid_prices": valid_prices,
        "valid_addresses": valid_addresses,
    }


with DAG(
    dag_id=DAG_ID,
    schedule="0 1 * * *",
    default_args=default_args,
    catchup=False,
    max_active_tasks=1,
    max_active_runs=1,
    tags=["s3", "raw"],
    description=SHORT_DESCRIPTION,
) as dag:
    start = EmptyOperator(
        task_id="start",
    )

    run_parser = DockerOperator(
        task_id="run_parser",
        image="flats-parser:2.0",
        container_name="flats_parser_container",
        api_version="auto",
        auto_remove="force",
        docker_url="unix://var/run/docker.sock",  # используем докер на хосте аирфлоу
        network_mode="data_network",
        mount_tmp_dir=False,
        tty=True,  # логи контейнера видны в логах аирфлоу
        mem_limit="3g",  # ограничение по памяти для контейнера
        shm_size="1g",  # для хрома внутри контейнера, чтобы не было ошибок с памятью при парсинге
        # параметры доступа к S3 через переменные окружения, чтобы внутри контейнера можно было сохранять в S3
        environment={
            "MINIO_ACCESS_KEY": "{{ conn.s3_conn.login }}",
            "MINIO_SECRET_KEY": "{{ conn.s3_conn.password }}",
            "MINIO_ENDPOINT_URL": "{{ conn.s3_conn.extra_dejson.endpoint_url }}",
            "MINIO_BUCKET_NAME": LAYER,
            "TZ": "Europe/Moscow",
            "EXECUTION_DATE": "{{ data_interval_start.in_timezone('Europe/Moscow').format('YYYY-MM-DD') }}",
        },
    )

    check_data_quality = PythonOperator(
        task_id="check_data_quality",
        python_callable=check_raw_data_quality,
    )

    end = EmptyOperator(
        task_id="end",
        outlets=[RAW_DATASET_CIAN_FLATS],  # триггер для запуска следующего DAG
    )

    start >> run_parser >> check_data_quality >> end
