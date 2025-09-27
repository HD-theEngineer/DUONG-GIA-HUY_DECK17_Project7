/*
WITH store_info AS (
    SELECT
        r.store_id AS store_key,
        ARRAY_AGG(DISTINCT loc.country_long) AS country_name,
        -- loc.country_short AS country_code,
        -- ARRAY_AGG(DISTINCT loc.region) AS region,
        -- ARRAY_AGG(DISTINCT loc.city) AS city,
        -- ARRAY_AGG(DISTINCT r.ip) AS ip,
    FROM
        {{ ref("stg_glamira__temp_raw_data") }} r
    LEFT JOIN
        {{ ref("stg_glamira__raw_ip2location")}} loc
    USING (ip)
    GROUP BY
        r.store_id
    ORDER BY 1
)*/

WITH store_info AS (
    SELECT
        DISTINCT store_id AS store_key,
        CONCAT('store-', CAST(store_id AS STRING)) AS store_name
    FROM
        {{ ref("stg_glamira__temp_raw_data") }}
    ORDER BY 1
)

SELECT * FROM store_info