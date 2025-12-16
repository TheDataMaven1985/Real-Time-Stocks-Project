with enriched as (
    SELECT symbol,
        cast(market_timestamp as date) as trade_date,
        day_low,
        day_high,
        current_price,
        first_value(current_price) over (
            partition by symbol,
            cast(market_timestamp as date)
            order by market_timestamp
        ) as candle_open,
        last_value(current_price) over (
            partition by symbol,
            cast(market_timestamp as date)
            order by market_timestamp rows between unbounded preceding and unbounded following
        ) as candle_close
    FROM {{ ref('silver_stock_quotes') }}
),
candles as (
    SELECT symbol,
        trade_date as candle_time,
        min(day_low) as candle_low,
        max(day_high) as candle_high,
        any_value(candle_open) as candle_open,
        any_value(candle_close) as candle_close,
        avg(current_price) as trend_line
    FROM enriched
    GROUP BY symbol,
        trade_date
),
ranked as (
    SELECT c.*,
        row_number() over (
            partition by symbol
            order by candle_time desc
        ) as rn
    FROM candles c
)
SELECT symbol,
    candle_time,
    candle_low,
    candle_high,
    candle_open,
    candle_close,
    trend_line
FROM ranked
WHERE rn <= 12
ORDER BY symbol,
    candle_time