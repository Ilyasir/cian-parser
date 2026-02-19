-- Этот SQL собирает стату по данным в silver, для проверки качетства данных после трансформации
SELECT
    COUNT(*) as total_rows,
    COUNT(distinct district) as all_districts,
    COUNT(distinct okrug) as okrugs,
    MIN(area) as min_area,
    MAX(area) as max_area,
    MIN(price) as min_price
FROM read_parquet('{{ silver_s3_key }}')