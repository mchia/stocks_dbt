SELECT
    tp.sector,
    DATE_TRUNC('month', dr.date) AS month,
    AVG(dr.daily_return) AS avg_return
FROM {{ ref('daily_returns') }} dr
INNER JOIN {{ source("stock_data", "ticker_profiles") }} tp
ON dr.ticker = tp.ticker
GROUP BY 1, 2