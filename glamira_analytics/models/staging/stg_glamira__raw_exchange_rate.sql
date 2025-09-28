with source as (
        select * from {{ source('glamira', 'raw_exchange_rate_new') }}
  ),
  renamed as (
      select
          {{ adapter.quote("country_code") }},
        {{ adapter.quote("symbol") }},
        {{ adapter.quote("currency_code") }},
        {{ adapter.quote("usd_exchange_rate") }},
        {{ adapter.quote("currency_name") }},
        {{ adapter.quote("country_name") }}

      from source
  )
  select * from renamed
    