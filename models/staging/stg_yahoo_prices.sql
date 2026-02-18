{{ config(
    materialized = 'view'
) }}

with source_data as (

    select *
    from {{ source('raw_yahoo', 'raw_yahoo_prices') }}

),

renamed as (

    select
        cast(symbol as string) as symbol,
        cast(trading_date as date) as trading_date,

        cast(open as float64) as open_price,
        cast(high as float64) as high_price,
        cast(low as float64) as low_price,
        cast(close as float64) as close_price,
        cast(adj_close as float64) as adjusted_close_price,

        cast(volume as int64) as volume,

        cast(currency as string) as currency,
        cast(exchange as string) as exchange,
        cast(source as string) as data_source,

        cast(ingestion_timestamp as timestamp) as ingestion_timestamp

    from source_data

)

select *
from renamed
