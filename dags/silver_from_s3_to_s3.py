import logging
import duckdb
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.sensors.external_task import ExternalTaskSensor

OWNER = "ilyas"
DAG_ID = "silver_from_s3_to_s3"

LAYER_SOURCE = "raw"
LAYER_TARGET = "silver"
SOURCE = "cian"

# S3
ACCESS_KEY = Variable.get("access_key")
SECRET_KEY = Variable.get("secret_key")
S3_ENDPOINT = Variable.get("s3_endpoint")

SHORT_DESCRIPTION = "Трансформация сырых JSONL данных в типизированный Parquet с помощью DuckDB и сохранение в S3"

default_args = {
    'owner': OWNER,
    "start_date": pendulum.datetime(2026, 1, 18, tz="Europe/Moscow"),
    'retries': 2,
    "retry_delay": pendulum.duration(minutes=10),
}


def get_and_transform_raw_data_to_silver_s3(**context):
    # Формируем полные пути для duckdb
    dt = context["data_interval_start"].in_timezone('Europe/Moscow')
    year = dt.year
    month = dt.strftime('%m')
    day = dt.strftime('%d')
    
    raw_s3_path = f"s3://{LAYER_SOURCE}/{SOURCE}/year={year}/month={month}/day={day}/flats.jsonl"
    silver_s3_path = f"s3://{LAYER_TARGET}/{SOURCE}/year={year}/month={month}/day={day}/flats.parquet"

    # Читаем SQL из файла
    with open('dags/sql/transform_silver.sql', 'r') as f:
        sql_template = f.read()

    con = duckdb.connect()

    sql_query = f"""
        SET TIMEZONE='Europe/Moscow';
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_url_style = 'path';
        SET s3_endpoint = '{S3_ENDPOINT}';
        SET s3_access_key_id = '{ACCESS_KEY}';
        SET s3_secret_access_key = '{SECRET_KEY}';
        SET s3_use_ssl = FALSE;

        {sql_template.format(raw_path=raw_s3_path, silver_path=silver_s3_path)}
    """

    con.execute(sql_query)
    con.close()

    logging.info(f"✅ Файл успешно сохранен: {silver_s3_path}")


with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 1 * * *",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["s3", "silver"],
    description=SHORT_DESCRIPTION,
) as dag:

    start = EmptyOperator(
        task_id="start",
    )

    sensor_on_raw_layer = ExternalTaskSensor(
        task_id="sensor_on_raw_layer",
        external_dag_id="raw_from_parser_to_s3",
        allowed_states=["success"],
        mode="reschedule",
        timeout=36000,  # длительность работы сенсора
        poke_interval=60  # частота проверки
    )

    transform_to_silver = PythonOperator(
        task_id="transform_to_silver",
        python_callable=get_and_transform_raw_data_to_silver_s3
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> sensor_on_raw_layer >> transform_to_silver >> end