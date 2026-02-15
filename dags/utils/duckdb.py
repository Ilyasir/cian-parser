import duckdb
from airflow.hooks.base import BaseHook
from airflow.utils.log.secrets_masker import mask_secret


def get_duckdb_s3_connection(conn_id: str = "s3_conn"):
    """Получение подключения к S3 для duckdb через Airflow Connection"""
    s3_conn = BaseHook.get_connection(conn_id)

    mask_secret(s3_conn.login)
    mask_secret(s3_conn.password)

    access_key = s3_conn.login
    secret_key = s3_conn.password
    endpoint_with_protocol = s3_conn.extra_dejson.get("endpoint_url", "")
    url_style = s3_conn.extra_dejson.get("addressing_style", "path")
    region = s3_conn.extra_dejson.get("region_name", "ru-central1")

    use_ssl = "true" if endpoint_with_protocol.startswith("https") else "false"
    endpoint = endpoint_with_protocol.replace("http://", "").replace("https://", "")

    con = duckdb.connect()
    # загружаем расширение для работы с S3
    con.execute("SET extension_directory = '/opt/airflow/duckdb_extensions';")
    con.execute("LOAD httpfs;")

    con.execute(f"""
        SET TimeZone = 'Europe/Moscow';
        SET s3_url_style = '{url_style}';
        SET s3_endpoint = '{endpoint}';
        SET s3_access_key_id = '{access_key}';
        SET s3_secret_access_key = '{secret_key}';
        SET s3_region = '{region}';
        SET s3_use_ssl = {use_ssl};
    """)
    return con
