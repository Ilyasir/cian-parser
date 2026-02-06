import logging
import duckdb
import pendulum
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowFailException

OWNER = "ilyas"
DAG_ID = "raw_from_parser_to_s3"

LAYER = "raw"

SHORT_DESCRIPTION = "DAG запускает контейнер парсера для сбора объявлений о квартирах и сохраняет результат в S3 в формате .jsonl"

default_args = {
    'owner': OWNER,
    "start_date": pendulum.datetime(2026, 1, 18, tz="Europe/Moscow"),
    'retries': 2,
    "retry_delay": pendulum.duration(hours=2),
}


def check_raw_data_quality(**context) -> dict[str, int]:
    """Проверка качества данных в S3 с помощью duckdb"""
    s3_conn = BaseHook.get_connection("s3_conn")
    # параметры подключения
    access_key = s3_conn.login
    secret_key = s3_conn.password
    s3_endpoint = s3_conn.extra_dejson.get("endpoint_url")

    dt = context["data_interval_start"].in_timezone('Europe/Moscow')
    year = dt.year
    month = dt.strftime('%m')
    day = dt.strftime('%d')
    
    s3_path = f"s3://{LAYER}/cian/year={year}/month={month}/day={day}/flats.jsonl"

    con = duckdb.connect()
    con.execute(
        f"""
        INSTALL httpfs; -- расширение для работы с S3/HTTP
        LOAD httpfs;
        SET s3_url_style = 'path';
        SET s3_endpoint = '{s3_endpoint.replace("http://", "")}'; -- duckdb сам подставляет http://
        SET s3_access_key_id = '{access_key}';
        SET s3_secret_access_key = '{secret_key}';
        SET s3_use_ssl = FALSE;
        """
    )

    logging.info("💻 Выполняю проверку данных")
    data_quality_results: tuple[int, int, int, int, int] = con.execute(
        f"""
            SELECT
                COUNT(*) as total_rows,
                COUNT(DISTINCT id) as unique_ids,
                COUNT(price) FILTER (WHERE price IS NOT NULL AND price != '') as valid_prices,
                COUNT(address) FILTER (WHERE address IS NOT NULL AND address != '') as valid_addresses,
                COUNT(metro) FILTER (WHERE metro IS NOT NULL AND metro != '') as valid_metro
            FROM read_json_auto('{s3_path}')
        """).fetchone()
    con.close()

    total_rows, unique_ids, valid_prices, valid_addresses, valid_metro = data_quality_results

    if total_rows == 0:
        raise AirflowFailException("Файл пустой!")
    # рейты
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

    logging.info(f"✅ Проверка пройдена. Всего строк: {total_rows}. "
                 f"Процент заполненных адресов: {valid_addresses_rate:.2%}. " 
                 f"Процент заполненных метро: {valid_metro_rate:.2%}")
    
    return {"total_rows": total_rows,
            "unique_ids": unique_ids,
            "valid_prices": valid_prices,
            "valid_addresses": valid_addresses}


with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 1 * * *",
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
        task_id='run_flats_parser',
        image='flats-parser:2.0',
        api_version='auto',
        auto_remove='success',
        docker_url="unix://var/run/docker.sock",
        network_mode="flats_analyze_default",
        mount_tmp_dir=False,
        tty=True, # логи контейнера видны в логах аирфлоу
        mem_limit='4g',
        shm_size='2g',
        environment={
            'MINIO_ACCESS_KEY': "{{ conn.s3_conn.login }}",
            'MINIO_SECRET_KEY': "{{ conn.s3_conn.password }}",
            'MINIO_ENDPOINT_URL': "{{ conn.s3_conn.extra_dejson.endpoint_url }}",
            'MINIO_BUCKET_NAME': LAYER,
            'TZ': 'Europe/Moscow',
            'EXECUTION_DATE': "{{ data_interval_start.in_timezone('Europe/Moscow').format('YYYY-MM-DD') }}",
        }
    )

    check_data_quality = PythonOperator(
        task_id='check_data_quality',
        python_callable=check_raw_data_quality,
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> run_parser >> check_data_quality >> end