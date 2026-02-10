import duckdb
from airflow.hooks.base import BaseHook


def get_duckdb_s3_connection(conn_id: str = "s3_conn"):
    """Получение подключения к S3 для duckdb через Airflow Connection"""
    # данные подключения из Airflow
    s3_conn = BaseHook.get_connection(conn_id)
    access_key = s3_conn.login
    secret_key = s3_conn.password
    # duckdb сам подставляет протокол
    endpoint = s3_conn.extra_dejson.get("endpoint_url", "").replace("http://", "").replace("https://", "")

    con = duckdb.connect()
    # загружаем расширения для работы с S3
    con.execute("LOAD httpfs;")

    con.execute(f"""
        SET TimeZone = 'Europe/Moscow';
        SET s3_url_style = 'path';
        SET s3_endpoint = '{endpoint}';
        SET s3_access_key_id = '{access_key}';
        SET s3_secret_access_key = '{secret_key}';
        SET s3_use_ssl = FALSE;
    """)
    return con
