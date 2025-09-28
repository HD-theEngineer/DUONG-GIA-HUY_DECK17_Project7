WITH dim_product__load AS (
    SELECT
        product_key,
        product_name,
        gender,
    FROM
        {{ ref('stg_glamira__products')}}
)

, dim_products__null_handle AS (
    SELECT
        product_key,
        COALESCE(product_name, 'XNA') AS product_name,
        COALESCE(gender, 'XNA') AS gender,
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
)

SELECT
    *
FROM
    dim_product__handle_invalid
