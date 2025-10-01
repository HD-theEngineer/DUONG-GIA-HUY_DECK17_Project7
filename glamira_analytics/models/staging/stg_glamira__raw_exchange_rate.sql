with source as (
        select * from {{ source('glamira', 'raw_exchange_rate') }}
  ),
  renamed as (
      select
          {{ adapter.quote("country_name") }},
        {{ adapter.quote("country_code") }},
        {{ adapter.quote("currency_symbol") }},
        {{ adapter.quote("currency_name") }},
        {{ adapter.quote("exchange_rate_to_usd") }}
      from source
  )
  select * from renamed
    