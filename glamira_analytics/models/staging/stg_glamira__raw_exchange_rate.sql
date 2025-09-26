with source as (
        select * from {{ source('glamira', 'raw_exchange_rate') }}
  ),
  renamed as (
      select
          {{ adapter.quote("symbol") }},
        {{ adapter.quote("code") }},
        {{ adapter.quote("currency_name") }},
        {{ adapter.quote("exchange_rate") }},
        {{ adapter.quote("country_code") }}

      from source
  )
  select * from renamed
    