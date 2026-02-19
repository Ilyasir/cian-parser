-- Этот SQL делает загрузку из silver в stage в pg, чтобы потом смержить
ATTACH '' AS flats_db (TYPE postgres, SECRET dwh_postgres); -- подключаем постгрес
-- очистка stage чтобы не ыбло дубликатов
TRUNCATE TABLE flats_db.gold.stage_flats;
-- читаем из silver и пишем в pg
INSERT INTO flats_db.gold.stage_flats (
    flat_hash, link, title, price, is_apartament, is_studio, area, 
    rooms_count, floor, total_floors, is_new_moscow, address, 
    city, okrug, district, metro_name, metro_min, metro_type, 
    parsed_at
)
SELECT
    flat_hash, link, title, price, is_apartament, is_studio, area, 
    rooms_count, floor, total_floors, is_new_moscow, address, 
    city, okrug::flats_db.gold.okrug_name, district,
    metro_name, metro_min, metro_type::flats_db.gold.transport_type, parsed_at
FROM read_parquet('{{silver_s3_key}}');