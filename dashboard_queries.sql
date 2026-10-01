-- Stock Market Streaming Analytics – Grafana/MySQL Queries

-- 1. Stock Price vs MA(5)
SELECT
    event_timestamp AS time,
    price AS "Price",
    moving_average AS "MA(5)"
FROM stock_market_data
WHERE symbol = '${stock}'
ORDER BY event_timestamp;

-- 2. Price Change % vs Trading Volume
SELECT
    event_timestamp AS time,
    percentage_change AS "Price Change %",
    volume AS "Volume"
FROM stock_market_data
WHERE symbol = '${stock}'
ORDER BY event_timestamp;

-- 3. Streaming Alert Analysis – All Stocks
SELECT
    symbol AS "Stock",
    alert_status AS "Alert Status",
    COUNT(*) AS "Number of Events"
FROM stock_market_data
GROUP BY symbol, alert_status
ORDER BY symbol, COUNT(*) DESC;

-- 4. Latest Streaming Metrics – All Stocks
SELECT
    event_timestamp AS "Time",
    symbol AS "Stock",
    price AS "Price",
    moving_average AS "MA(5)",
    price_change AS "Price Change",
    percentage_change AS "Change %",
    volume AS "Volume",
    alert_status AS "Alert"
FROM stock_market_data
WHERE id IN (
    SELECT MAX(id)
    FROM stock_market_data
    GROUP BY symbol
)
ORDER BY symbol;

-- 5. All Stock Price History
SELECT event_timestamp AS time, symbol, price
FROM stock_market_data
ORDER BY event_timestamp;

-- 6. Alert Summary
SELECT alert_status AS "Alert Status", COUNT(*) AS "Count"
FROM stock_market_data
GROUP BY alert_status
ORDER BY COUNT(*) DESC;
