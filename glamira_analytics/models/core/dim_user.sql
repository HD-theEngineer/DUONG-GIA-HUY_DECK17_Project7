WITH dim_user__load AS (
    SELECT
        *
    FROM
        {{ ref("stg_glamira__user")}}
)

, dim_user__handle_null AS (
    SELECT
        COALESCE(user_key, -1) AS user_key,
        COALESCE(user_email_address, 'XNA') AS user_email_address,
    FROM
        dim_user__load
)

, dim_user__deduplicate AS (
    SELECT
        DISTINCT user_key,
        user_email_address,
    FROM
        dim_user__handle_null
)

SELECT
    *
FROM
    dim_user__deduplicate
ORDER BY user_key