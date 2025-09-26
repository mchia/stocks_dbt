SELECT
    ticker,
    date,
    close,
    LAG(close) OVER (PARTITION BY ticker ORDER BY date) AS prev_close,
    (close / LAG(close) OVER (PARTITION BY ticker ORDER BY date) - 1) AS daily_return
FROM {{ source("stock_data", "ticker_data") }}
WHERE close IS NOT NULL
QUALIFY 
    LAG(close) OVER (PARTITION BY ticker ORDER BY date) IS NOT NULL
    AND (close / LAG(close) OVER (PARTITION BY ticker ORDER BY date) - 1) != 0