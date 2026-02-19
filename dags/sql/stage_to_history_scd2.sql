-- Этот SQL реализует SCD2, сохраняет фулл историю изменений цены
UPDATE gold.history_flats as hf -- закрываем старые записи, со старой ценой
SET
    effective_to = stg.parsed_at,
    is_active = FALSE
FROM gold.stage_flats as stg
WHERE hf.flat_hash = stg.flat_hash -- хэши совпадают (квартира уже есть в истории), и если цена изменилась
    AND hf.is_active = TRUE
    AND hf.price != stg.price;

-- вставляем квариры, которые вообще не были в истории, и которые с новой ценой
INSERT INTO gold.history_flats (
    flat_hash, link, title, price, is_apartament, is_studio, area, 
    rooms_count, floor, total_floors, is_new_moscow, address, 
    city, okrug, district, metro_name, metro_min, metro_type, 
    parsed_at, effective_from, is_active
)
SELECT 
    stg.flat_hash, stg.link, stg.title, stg.price, stg.is_apartament, stg.is_studio, stg.area, 
    stg.rooms_count, stg.floor, stg.total_floors, stg.is_new_moscow, stg.address, 
    stg.city, stg.okrug, stg.district, stg.metro_name, stg.metro_min, stg.metro_type, 
    stg.parsed_at,
    -- новая запись становится активной с момента ее парсинга
    stg.parsed_at as effective_from,
    TRUE as is_active
FROM gold.stage_flats as stg
LEFT JOIN gold.history_flats as hf -- джоиним по хэшу, чтобы понять, есть ли уже такая квартира в истории
    ON stg.flat_hash = hf.flat_hash AND hf.is_active = TRUE
WHERE hf.flat_hash IS NULL -- если квартиры вообще нет в истории
   OR hf.price != stg.price; -- или есть, но цена изменилась

-- чекаем результат после мержа, сколько в stage и сколько всего активных в истории, видно в логах Airflow
SELECT 
    (SELECT count(*) FROM gold.stage_flats) as total_in_stage,
    (SELECT count(*) FROM gold.history_flats WHERE is_active = TRUE) as active_records_after_merge;