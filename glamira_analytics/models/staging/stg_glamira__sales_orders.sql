WITH stg_glamira__order_rename AS (
    SELECT
        CAST(FORMAT_DATE('%Y%m%d', EXTRACT(DATE FROM r.time_stamp)) AS INTEGER) AS date_key,
        CASE
            WHEN TRIM(r.user_id_db) = '' THEN NULL
            ELSE SAFE_CAST(r.user_id_db AS INTEGER)
        END AS user_key,
        SAFE_CAST(r.store_id AS INTEGER) AS store_key,
        r.ip AS ip_address,
        UPPER(REGEXP_EXTRACT(current_url, r'\.([a-z]+)/')) AS country_code,
        CAST(CAST(TRIM(r.order_id) AS FLOAT64) AS INT64) AS order_key,
        cart.product_id AS product_key,
        cart.amount AS product_amount,
        SAFE_CAST(
            CASE
                WHEN cart.price LIKE '%,%' AND cart.price NOT LIKE '%.%' THEN
                    REPLACE(cart.price, ',', '.')
                WHEN STRPOS(cart.price, ',') > STRPOS(cart.price, '.') THEN
                    REPLACE(REPLACE(cart.price, '.', ''), ',', '.')
                WHEN STRPOS(cart.price, '.') > STRPOS(cart.price, ',') THEN
                    REPLACE(cart.price, ',', '')
                ELSE cart.price
            END 
            AS FLOAT64
        ) AS product_price,
        TRIM(
            REGEXP_REPLACE(
                cart.currency,r"[0-9,\.\s'’]+",''
            )
        ) AS currency_symbol,
        cart.option AS product_option
    FROM
        {{ ref("stg_glamira__temp_raw_data")}} r,
        UNNEST(r.cart_products) AS cart
)

, stg_glamira__exchange_load AS (
    SELECT DISTINCT
        currency_symbol AS currency_symbol,
        country_short_name AS country_short_name,
        exchange_rate_to_usd AS exchange_rate_to_usd
    FROM
        {{ ref('stg_glamira__location') }}
)

, tmp AS (
    SELECT DISTINCT
        country_code AS country_code,
        currency_symbol AS currency_symbol,
    FROM
        stg_glamira__order_rename
)

, tmp2 AS (
    SELECT
        o.currency_symbol AS raw_data,
        e.currency_symbol AS std_data,
        e.exchange_rate_to_usd AS exchange_rate
    FROM
        tmp o
    FULL OUTER JOIN
        stg_glamira__exchange_load e
    USING (currency_symbol)
)

/*
, stg_glamira__order_join_products AS (
    SELECT
        o.date_key AS date_key,
        o.user_key AS user_key,
        o.store_key AS store_key,
        l.ip_address AS ip_address,
        l.location_key AS location_key,
        o.order_key AS order_key,
        o.product_key AS product_key,
        o.product_amount AS product_amount,
        o.product_option AS product_option,
        o.product_price AS product_price,
        o.product_currency AS product_currency,
        e.exchange_rate_to_usd AS exchange_rate_to_usd,
        o.product_price*e.exchange_rate_to_usd AS product_price_usd
    FROM
        stg_glamira__order_rename o
    LEFT JOIN
        stg_glamira__exchange_rename e
    USING (ip_address)
)

, stg_glamira__product_grouping AS (
    SELECT
        date_key,
        user_key,
        store_key,
        ip_address,
        location_key,
        order_key,
        product_key,
        product_currency,
        SUM(product_amount) AS amount_per_product_per_order,
        SUM(product_price_usd) price_per_product_per_order
    FROM
        stg_glamira__order_join_products
    GROUP BY
        date_key,
        user_key,
        store_key,
        ip_address,
        location_key,
        order_key,
        product_key,
        product_currency
    
)

, stg_glamira__order_gen_key AS (
SELECT
    FARM_FINGERPRINT(order_key || product_key) AS sales_order_key,
    *
FROM
    stg_glamira__product_grouping
-- WHERE
    -- product_name IS NULL
    -- AND order_product_price != 0
    -- AND ARRAY_LENGTH(order_option) > 0
)
*/
SELECT * FROM tmp WHERE currency_symbol IN ('$')
-- WHERE order_key = 720248396