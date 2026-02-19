-- Этот SQL собирает статистику по сырым данным, для проверки сразу после парсинга и загрузки в S3
SELECT
    COUNT(*) as total_rows,
    COUNT(DISTINCT id) as unique_ids,
    COUNT(price) FILTER (WHERE price IS NOT NULL AND price != '') as valid_prices,
    COUNT(address) FILTER (WHERE address IS NOT NULL AND address != '') as valid_addresses,
    COUNT(metro) FILTER (WHERE metro IS NOT NULL AND metro != '') as valid_metro,
    COUNT(description) FILTER (WHERE description IS NOT NULL AND description != '') as valid_description
FROM read_json_auto('{{raw_s3_key}}')