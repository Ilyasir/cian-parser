CREATE TABLE IF NOT EXISTS gold.dm_district_current (
    okrug gold.okrug_name,
    district VARCHAR(100),
    total_flats INT,
	avg_price BIGINT,
    avg_price_per_meter BIGINT,
    median_price_per_meter BIGINT,
	min_price BIGINT,
	max_price BIGINT,
	updated_at TIMESTAMP DEFAULT NOW()
);