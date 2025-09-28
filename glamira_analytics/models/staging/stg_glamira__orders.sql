WITH stg_glamira__order_rename AS (
    SELECT
        EXTRACT(DATE FROM r.time_stamp) AS order_date,
        r.user_id_db AS user_key,
        r.store_id AS store_key,
        r.ip AS ip_address,
        r.order_id AS order_key,
        cart.product_id AS product_key,
        cart.amount AS order_amount,
        SAFE_CAST(
        CASE
            WHEN cart.price LIKE '%,%' AND cart.price NOT LIKE '%.%' THEN
                REPLACE(cart.price, ',', '.')
            WHEN STRPOS(cart.price, ',') > STRPOS(cart.price, '.') THEN
                REPLACE(REPLACE(cart.price, '.', ''), ',', '.')
            WHEN STRPOS(cart.price, '.') > STRPOS(cart.price, ',') THEN
                REPLACE(cart.price, ',', '')
            ELSE cart.price
        END AS FLOAT64) AS order_product_price,
        cart.option AS order_option
    FROM
        {{ ref("stg_glamira__temp_raw_data")}} r,
        UNNEST(r.cart_products) AS cart
)

, stg_glamira__location_load AS (
    SELECT
        *
    FROM
        {{ref("stg_glamira__location")}}
)

, stg_glamira__order_join_products AS (
    SELECT
        CAST(FORMAT_DATE('%Y%m%d', order_date) AS INTEGER) AS date_key,
        user_key,
        store_key,
        ip_address,
        order_key,
        product_key,
        order_amount,
        order_option,
        order_product_price,
        exchange_rate_to_usd,
        order_product_price*exchange_rate AS order_product_price_usd
    FROM
        stg_glamira__order_rename
    LEFT JOIN
        stg_glamira__location_load
    USING (ip_address)
)

, stg_glamira__order_gen_key AS (
SELECT
    FARM_FINGERPRINT(order_key || product_key) AS sales_order_key,
    *
FROM
    stg_glamira__order_join_products
-- WHERE
    -- product_name IS NULL
    -- AND order_product_price != 0
    -- AND ARRAY_LENGTH(order_option) > 0
)

SELECT DISTINCT * FROM stg_glamira__order_gen_key