WITH fact_sales_order__rename AS (
    SELECT
        *
    FROM
        {{ref("stg_glamira__sales_orders")}}
)

, fact_sales_order__handle_null AS (
    SELECT
        sales_order_key,
        COALESCE(date_key, -1) AS date_key,
        COALESCE(user_key, -1) AS user_key,
        COALESCE(store_key, -1) AS store_key,
        COALESCE(ip_address, 'XNA') AS ip_address,
        COALESCE(location_key, -1) AS location_key,
        COALESCE(order_key, -1) AS order_key,
        COALESCE(product_key, -1) AS product_key,
        COALESCE(product_amount, 0) AS product_amount,
        CASE 
            WHEN ARRAY_LENGTH(product_option) = 0 THEN 
                [STRUCT(
                    'XNA' AS option_label,
                    '-1' AS option_id,
                    'XNA' AS value_label, 
                    '-1' AS value_id
                )]
            ELSE product_option 
        END AS product_option,
        COALESCE(product_price_usd, 0) AS product_price_usd
    FROM
        fact_sales_order__rename
)

, fact_sales_order__handle_invalid AS (
    SELECT
        *
    FROM
        fact_sales_order__handle_null
    UNION ALL
    SELECT
        -1 AS sales_order_key,
        -1 AS date_key,
        -1 AS user_key,
        -1 AS store_key,
        'XNA' AS ip_address,
        -1 AS location_key,
        -1 AS order_key,
        -1 AS product_key,
        0 AS product_amount,
        [STRUCT(
            'XNA' AS option_label,
            '-1' AS option_id,
            'XNA' AS value_label, 
            '-1' AS value_id
        )] AS product_option,
        0 AS product_price_usd
)

SELECT
    *
FROM
    fact_sales_order__handle_invalid
WHERE
    sales_order_key = 2741169628621187074