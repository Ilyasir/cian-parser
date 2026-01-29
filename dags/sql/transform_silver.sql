COPY(
    SELECT
        id::BIGINT as id,
        link::TEXT as link,
        title::VARCHAR as title,
        --
        CASE
            WHEN title ILIKE '%апартаменты%' THEN TRUE
            ELSE FALSE
        END as is_apartament,
        --
        CASE
            WHEN title ILIKE '%студия%' THEN TRUE
            ELSE FALSE
        END as is_studio,
        -- площадь из заголовка
        replace(NULLIF(regexp_extract(title, '(\d+[.,]?\d*)\s*м²', 1), ''), ',', '.')::NUMERIC(10, 2) as area,
        -- комнатность
        CASE 
            WHEN title ILIKE '%студия%' THEN 0
            WHEN title ILIKE '%своб%' THEN 0
            ELSE NULLIF(regexp_extract(title, '^(\d+)', 1), '')::INT
        END as rooms_count,
        -- этажи
        NULLIF(regexp_extract(title, '(\d+)/\d+\s*этаж', 1), '')::INT as floor,
        NULLIF(regexp_extract(title, '\d+/(\d+)\s*этаж', 1), '')::INT as total_floors,
        regexp_replace(price, '[^0-9]', '', 'g')::BIGINT as price,
        address::TEXT as address,
        -- разбиваем адрес
        trim(SPLIT_PART(address, ',', 1))::VARCHAR as city,
        regexp_extract(trim(SPLIT_PART(address, ',', 2)), '^([А-Я]+)', 1)::VARCHAR as okrug,
        trim(SPLIT_PART(address, ',', 3))::VARCHAR as district,
        -- вся инфа о метро
        trim(regexp_extract(metro, '^(.*?)\d+\s+минут', 1)) as metro_name,
        NULLIF(regexp_extract(metro, '(\d+)\s+минут', 1), '')::INT as metro_min,
        CASE
            WHEN metro LIKE '%пешком%' THEN 'walk'
            WHEN metro LIKE '%транс%' THEN 'transport'
        END as metro_type,
        parsed_at::TIMESTAMP as parsed_at,
        description::TEXT as description
        FROM read_json_auto('{raw_path}')
) TO '{silver_path}' (FORMAT PARQUET);