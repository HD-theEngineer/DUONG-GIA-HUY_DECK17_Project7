WITH dim_product__load AS (
    SELECT
        product_key,
        product_name,
        gender,
        price_usd,
        min_price_usd,
        max_price_usd
    FROM
        {{ ref('stg_glamira__products')}}
)

, dim_products__null_handle AS (
    SELECT
        product_key,
        COALESCE(product_name, 'XNA') AS product_name,
        COALESCE(gender, 'XNA') AS gender,
        COALESCE(price_usd, CAST(0 AS FLOAT64)) AS price_usd,
        COALESCE(min_price_usd, CAST(0 AS FLOAT64)) AS min_price_usd,
        COALESCE(max_price_usd, CAST(0 AS FLOAT64)) AS max_price_usd,
    FROM
        dim_product__load
)

, dim_product__handle_invalid AS (
    SELECT
        *
    FROM
        dim_products__null_handle
    UNION ALL
    SELECT
        -1 AS product_key,
        'XNA' AS product_name,
        'XNA' AS gender,
        CAST(0 AS FLOAT64) AS price_usd,
        CAST(0 AS FLOAT64) AS min_price_usd,
        CAST(0 AS FLOAT64) AS max_price_usd
)

SELECT
    *
FROM
    dim_product__handle_invalid
