WITH stg_glamira__user_rename AS (
    SELECT
        user_id_db AS user_key,
        email_address AS user_email_address,
        order_id,
        ip,
    FROM
        {{ ref("stg_glamira__temp_raw_data")}}
)

, stg_glamira__user_handle_invalid AS (
    SELECT
        CASE 
            WHEN TRIM(user_key) = ''
            THEN NULL
            ELSE CAST(user_key AS INTEGER)
        END AS user_key,
        CASE 
            WHEN TRIM(user_email_address) = ''
            THEN NULL
            ELSE user_email_address
        END AS user_email_address,
    FROM
        stg_glamira__user_rename
    ORDER BY 1 DESC
)

SELECT
    *
FROM
    stg_glamira__user_handle_invalid