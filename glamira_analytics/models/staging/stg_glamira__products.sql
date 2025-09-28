WITH stg_glamira__products_rename AS (
    SELECT
        product_id AS product_key,
        name AS product_name,
        price,
        min_price,
        max_price,
        UPPER(REGEXP_EXTRACT(
            REPLACE(url, 'https://www.glamira', ''),
            r'\.([a-z]{2})/'
        )) AS country_code,
         TRIM(REGEXP_REPLACE(TRIM(min_price_format), r"[0-9,\.\s'’]+",'')) AS symbol,
        gender
    FROM
        {{ ref("stg_glamira__raw_crawl_products")}}
)

, stg_glamira__products_handle_null AS(
    SELECT
        product_key,
        product_name,
        price,
        min_price,
        max_price,
        CASE
            WHEN country_code IS NULL
            THEN 'US'
            ELSE country_code
        END AS country_code,
        symbol,
        CASE 
            WHEN gender = 'false' 
            THEN NULL
            ELSE gender END AS gender
    FROM
        stg_glamira__products_rename
)

, stg_glamira__exchange_rename AS (
    SELECT
        TRIM(REGEXP_REPLACE(symbol, r"[0-9,\.\s'’]+",'')) AS symbol,
        country_code,
        usd_exchange_rate

    FROM
        {{ ref('stg_glamira__raw_exchange_rate') }}
)

, stg_glamira__currency_mapping AS (
    SELECT
        cr.*,
        ex.usd_exchange_rate AS exchange_rate,
    FROM
        stg_glamira__products_handle_null cr
    LEFT JOIN stg_glamira__exchange_rename ex
    USING(country_code)
)

, stg_glamira__product_usd AS (
    SELECT
        product_key,
        product_name,
        gender,
        CONCAT(symbol, '-', country_code) as price_origin,
        exchange_rate,
        price,
        min_price,
        max_price,
        SAFE_CAST(price AS FLOAT64)*exchange_rate AS price_usd,
        SAFE_CAST(min_price AS FLOAT64)*exchange_rate AS min_price_usd,
        SAFE_CAST(max_price AS FLOAT64)*exchange_rate AS max_price_usd,
        
    FROM
        stg_glamira__currency_mapping
)

SELECT
    *
FROM
    stg_glamira__product_usd