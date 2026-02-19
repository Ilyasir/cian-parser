from airflow.datasets import Dataset

# Датасеты для разных слоев
RAW_DATASET_CIAN_FLATS = Dataset("s3://raw/cian/flats_data")
SILVER_DATASET_CIAN_FLATS = Dataset("s3://silver/cian/flats_data")
GOLD_DATASET_HISTORY = Dataset("pg://postgres_dwh/gold/history_flats")
