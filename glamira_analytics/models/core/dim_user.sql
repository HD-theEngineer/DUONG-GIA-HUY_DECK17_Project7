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

SELECT
    *
FROM
    dim_user__handle_null
ORDER BY user_key