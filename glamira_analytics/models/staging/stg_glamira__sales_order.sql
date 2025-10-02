WITH stg_glamira__order_rename AS (
    SELECT
        CAST(
            FORMAT_DATE(
                '%Y%m%d', EXTRACT(DATE FROM r.time_stamp)
            ) 
        AS INTEGER)                                                         AS date_key,
        SAFE_CAST(local_time AS DATETIME)                                   AS local_time,
        CASE
            WHEN TRIM(r.user_id_db) = '' 
                OR TRIM(r.user_id_db) = '-' 
                THEN NULL
            ELSE SAFE_CAST(r.user_id_db AS INTEGER)
        END                                                                 AS user_key,
        SAFE_CAST(r.store_id AS INTEGER)                                    AS store_key,
        r.ip                                                                AS ip_address,
        UPPER(REGEXP_EXTRACT(current_url, r'\.([a-z]+)/'))                  AS country_short_name,
        SAFE_CAST(SAFE_CAST(TRIM(r.order_id) AS FLOAT64) AS INTEGER)        AS order_key,
        SAFE_CAST(cart.product_id AS INTEGER)                               AS product_key,
        SAFE_CAST(cart.amount AS INTEGER)                                   AS product_amount,
        SAFE_CAST(
            CASE
                WHEN TRIM(cart.price) = '' OR cart.price IS NULL THEN '0'
                WHEN cart.price LIKE '%,%' AND cart.price NOT LIKE '%.%' 
                    THEN REPLACE(cart.price, ',', '.')
                WHEN STRPOS(cart.price, ',') > STRPOS(cart.price, '.')
                    THEN REPLACE(REPLACE(cart.price, '.', ''), ',', '.')
                WHEN STRPOS(cart.price, '.') > STRPOS(cart.price, ',')
                    THEN REPLACE(cart.price, ',', '')
                ELSE cart.price
            END
        AS FLOAT64)                                                         AS product_price,
        TRIM(
            REGEXP_REPLACE(
                cart.currency,r"[0-9,\.\s'’]+",''
            )
        )                                                                   AS currency_symbol,
        cart.option                                                         AS product_option
    FROM
        {{ ref("stg_glamira__temp_raw_data")}} r,
        UNNEST(r.cart_products) AS cart
    WHERE collection = 'checkout_success'
)

, stg_glamira__location_key_load AS (
    SELECT DISTINCT
        location_key AS location_key,
        ip_address AS ip_address
    FROM
        {{ ref('stg_glamira__location') }}
)

, stg_glamira__enrich_location_key AS (
    SELECT
        r.*,
        l.location_key AS location_key
    FROM
        stg_glamira__order_rename r
    LEFT JOIN
        stg_glamira__location_key_load l
    USING (ip_address)
)

, stg_glamira__value_standardize AS (
    SELECT
        date_key,
        local_time,
        user_key,
        ip_address,
        store_key,
        location_key,
        country_short_name,
        order_key,
        product_key,
        product_option,
        product_amount,
        product_price,
        CASE 
            WHEN country_short_name = 'MX' AND currency_symbol = 'MXN$' THEN 'MXN'
            WHEN country_short_name = 'AR' AND currency_symbol = '$' THEN 'AR$'
            WHEN country_short_name = 'HK' AND currency_symbol IN ('HKD$','$') THEN 'HK$'
            WHEN country_short_name = 'NO' AND currency_symbol = 'kr' THEN 'NOkr'
            WHEN country_short_name = 'SE' AND currency_symbol = 'kr' THEN 'SEKr'
            WHEN country_short_name = 'DK' AND currency_symbol = 'kr' THEN 'DKkr'
            WHEN country_short_name IS NULL AND currency_symbol = 'kr' THEN 'SEKr'
            WHEN country_short_name = 'CN' AND currency_symbol = '￥' THEN 'CN￥'
            WHEN country_short_name = 'JP' AND currency_symbol = '￥' THEN 'JP￥'
            WHEN country_short_name = 'DE' AND TRIM(currency_symbol) = '' THEN '€'
            WHEN country_short_name = 'AE' AND TRIM(currency_symbol) = '' THEN 'AED'
            WHEN country_short_name = 'ZA' AND TRIM(currency_symbol) = '' THEN 'ZAR'
            WHEN country_short_name = 'RO' AND TRIM(currency_symbol) = '' THEN 'RON'
            WHEN country_short_name IN ('EC','PR') AND TRIM(currency_symbol) = 'USD$' THEN '$'
            ELSE currency_symbol
        END AS currency_symbol
    FROM
        stg_glamira__enrich_location_key
)

, stg_glamira__product_grouping AS (
    SELECT
                                date_key,
                                user_key,
                                store_key,
                                location_key,
                                order_key,
                                product_key,
                                local_time,
                                ip_address,
                                product_option,
                                currency_symbol,
        SUM(product_amount)     AS product_amount,
        AVG(
            CASE
                WHEN product_price = 0 THEN NULL
                ELSE product_price
            END
        )                       AS product_unit_price
    FROM
        stg_glamira__value_standardize
    GROUP BY
        date_key,
        user_key,
        ip_address,
        store_key,
        location_key,
        order_key,
        product_key,
        local_time,
        product_option,
        currency_symbol
)

, stg_glamira__exchange_load AS (
    SELECT DISTINCT
        TRIM(REGEXP_REPLACE(currency_symbol, r"[0-9,\s'’]+",''))            AS currency_symbol,
        SAFE_CAST(exchange_rate_to_usd AS FLOAT64)                          AS exchange_rate_to_usd
    FROM
        {{ ref("stg_glamira__raw_exchange_rate") }}
    WHERE
        currency_symbol <> 'RD$'
)

, stg_glamira__join_exchange_rate AS (
    SELECT
        o.date_key                                      AS date_key,
        o.user_key                                      AS user_key,
        o.store_key                                     AS store_key,
        o.location_key                                  AS location_key,
        o.order_key                                     AS order_key,
        o.product_key                                   AS product_key,
        o.local_time                                    AS local_time,
        o.ip_address                                    AS ip_address,
        o.product_option                                AS product_option,
        o.currency_symbol                               AS currency_symbol,
        o.product_amount                                AS product_amount,
        o.product_unit_price                            AS product_unit_price,
        e.exchange_rate_to_usd                          AS exchange_rate_to_usd,
        o.product_unit_price * e.exchange_rate_to_usd   AS product_price_usd
    FROM
        stg_glamira__product_grouping o
    LEFT JOIN
        stg_glamira__exchange_load e
    USING (currency_symbol)
)

, stg_glamira__order_gen_key AS (
SELECT
    FARM_FINGERPRINT(
        location_key || 
        order_key || 
        product_key || 
        FORMAT_TIME(
            '%H%M%S', EXTRACT(TIME FROM local_time)
        )
    ) AS sales_order_key,
    *
FROM
    stg_glamira__join_exchange_rate
)

SELECT 
    * 
FROM 
    stg_glamira__order_gen_key