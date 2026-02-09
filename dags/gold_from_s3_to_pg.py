import logging
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook
from airflow.sensors.external_task import ExternalTaskSensor
from utils.duckdb import get_duckdb_s3_connection

OWNER = "ilyas"
DAG_ID = "gold_from_s3_to_pg"

LAYER_SOURCE = "silver"
LAYER_TARGET = "gold"

SHORT_DESCRIPTION = ""

default_args = {
    'owner': OWNER,
    "start_date": pendulum.datetime(2026, 1, 18, tz="Europe/Moscow"),
    'retries': 2,
    "retry_delay": pendulum.duration(minutes=10),
}


def load_silver_data_from_s3_to_pg(**context) -> None:
    dt = context["data_interval_start"].in_timezone('Europe/Moscow')
    silver_s3_key = f"s3://{LAYER_SOURCE}/cian/year={dt.year}/month={dt.strftime('%m')}/day={dt.strftime('%d')}/flats.parquet"
    
    pg_conn = BaseHook.get_connection("pg_conn")
    con = get_duckdb_s3_connection("s3_conn")

    logging.info(f"💻 Загружаю данные из {silver_s3_key} в stage")
    con.execute(
        f"""
        INSTALL postgres;
        LOAD postgres;
        CREATE SECRET dwh_postgres (
            TYPE postgres,
            HOST '{pg_conn.host}',
            PORT {pg_conn.port},
            DATABASE '{pg_conn.schema}',
            USER '{pg_conn.login}',
            PASSWORD '{pg_conn.password}'
        );

        ATTACH '' AS flats_db (TYPE postgres, SECRET dwh_postgres);

        TRUNCATE TABLE flats_db.gold.stage_flats;
        
        INSERT INTO flats_db.gold.stage_flats (
            flat_id, link, title, price, is_apartament, is_studio, area, 
            rooms_count, floor, total_floors, is_new_moscow, address, 
            city, okrug, district, metro_name, metro_min, metro_type, 
            parsed_at
        )
        SELECT 
            id, link, title, price, is_apartament, is_studio, area, 
            rooms_count, floor, total_floors, is_new_moscow, address, 
            city, okrug, district, metro_name, metro_min, metro_type, 
            parsed_at
        FROM read_parquet('{silver_s3_key}');
        """
    )

    count_rows = con.execute("SELECT count(*) FROM flats_db.gold.stage_flats").fetchone()[0]
    logging.info(f"✅ Успешно перенесено в pg: {count_rows} строк.")
    con.close()


with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 1 * * *",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["pg", "gold"],
    description=SHORT_DESCRIPTION,
) as dag:

    start = EmptyOperator(
        task_id="start",
    )

    sensor_on_silver_layer = ExternalTaskSensor(
        task_id="sensor_on_silver_layer",
        external_dag_id="silver_from_s3_to_s3",
        allowed_states=["success"],
        mode="reschedule",
        timeout=36000,
        poke_interval=60
    )

    load_silver_to_stage=PythonOperator(
        task_id="load_silver_to_stage",
        python_callable=load_silver_data_from_s3_to_pg
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> sensor_on_silver_layer >> load_silver_to_stage >> end