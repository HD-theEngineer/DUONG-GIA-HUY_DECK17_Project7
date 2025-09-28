/*
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
*/
WITH stg_glamira__raw_data_load AS (
    SELECT
        cart_products AS cart_products
    FROM
        {{ref("stg_glamira__temp_raw_data")}}
)

, stg_glamira__raw_extract_product_id AS (
    SELECT
        cart.product_id AS product_id
    FROM
        stg_glamira__raw_data_load r,
        UNNEST(r.cart_products) AS cart
)

, stg_glamira__raw_unique_product_id AS (
    SELECT DISTINCT
        product_id AS product_key
    FROM
        stg_glamira__raw_extract_product_id
)

, stg_glamira__products_crawl_rename AS (
    SELECT
        product_id AS product_key,
        name AS product_name,
        gender
    FROM
        {{ ref("stg_glamira__raw_crawl_products")}}
)

, stg_glamira__products_join AS (
    SELECT
        *
    FROM
        stg_glamira__raw_unique_product_id
    FULL OUTER JOIN
        stg_glamira__products_crawl_rename
    USING(product_key)
)

, stg_glamira__products_handle_null AS(
    SELECT
        product_key,
        product_name,
        CASE 
            WHEN gender = 'false' 
            THEN 'unisex'
            ELSE gender END AS gender
    FROM
        stg_glamira__products_join
)

SELECT
    *
FROM
    stg_glamira__products_handle_null