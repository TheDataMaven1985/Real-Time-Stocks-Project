SELECT symbol,
    current_price,
    change_amount,
    change_percent
FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY symbol
                ORDER BY market_timestamp DESC
            ) AS row_num
        FROM {{ ref('silver_stock_quotes') }}
    ) t
WHERE row_num = 1