import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from utils.datasets import GOLD_DATASET_HISTORY

OWNER = "ilyas"
DAG_ID = "gold_mart_district_stats"

SHORT_DESCRIPTION = ""

LONG_DESCRIPTION = """
"""


default_args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2026, 1, 18, tz="Europe/Moscow"),
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=10),
}


with DAG(
    dag_id=DAG_ID,
    schedule=[GOLD_DATASET_HISTORY],
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["mart", "gold", "pg"],
    description=SHORT_DESCRIPTION,
    doc_md=LONG_DESCRIPTION,
) as dag:
    start = EmptyOperator(
        task_id="start",
    )

    build_dm_district_current = SQLExecuteQueryOperator(
        task_id="build_dm_district_current",
        conn_id="pg_conn",
        autocommit=True,
        sql="""
            TRUNCATE TABLE gold.dm_district_current;

            INSERT INTO gold.dm_district_current (
                okrug, district, total_flats, avg_price, avg_price_per_meter, 
                median_price_per_meter, min_price, max_price
            )
            SELECT
                okrug,
                district,
                count(*) as total_flats,
                round(avg(price)) as avg_price,
                round(avg(price / area)) as avg_price_per_meter,
                round(percentile_cont(0.5) WITHIN GROUP (ORDER BY price / area))::BIGINT as median_price_per_meter,
                min(price) as min_price,
                max(price) as max_price
            FROM gold.history_flats
            WHERE is_active = true
            GROUP BY okrug, district;
        """,
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> build_dm_district_current >> end
