{{ config(
    materialized = 'table'
) }}

with price_data as (

    select
        symbol,
        trading_date,
        close_price
    from {{ ref('stg_yahoo_prices') }}

),

returns_calc as (

    select
        symbol,
        trading_date,
        close_price,
        lag(close_price) over (partition by symbol order by trading_date) as prev_close,
        round(
            (close_price - lag(close_price) over (partition by symbol order by trading_date))
            / lag(close_price) over (partition by symbol order by trading_date), 6
        ) as daily_return
    from price_data

)

select *
from returns_calc
where prev_close is not null
