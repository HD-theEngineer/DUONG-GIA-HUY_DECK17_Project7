WITH fact_sales_order__rename AS (
    SELECT
        *
    FROM
        {{ref("stg_glamira__orders")}}
)

, dim_location__load AS (
    SELECT
        *
    FROM
        {{ref("dim_location")}}
)

, fact_sales_order__join_location AS (
    SELECT
        f.sales_order_key,
        f.date_key,
        f.user_key,
        f.store_key,
        l.location_key AS location_key,
        f.order_key,
        f.product_key,
        f.order_amount,
        f.order_option,
        f.order_product_price_usd,
    FROM
        fact_sales_order__rename f
    FULL OUTER JOIN
        dim_location__load l
    USING (ip_address)
    
)

SELECT
    *
FROM
    fact_sales_order__rename