SELECT
    region,
    COUNT(DISTINCT order_id) AS TotalOrders,
    SUM(CASE
        WHEN payment_amount_ghs IS NULL THEN total_item_value_ghs
        ELSE payment_amount_ghs
    END) AS RegionalRevenue
FROM
    ProductUsage
GROUP BY
    region
ORDER BY
    RegionalRevenue DESC;
    
    -- Payment Method Performance
SELECT
    payment_method,
    COUNT(DISTINCT order_id) AS OrdersCount,
    SUM(CASE
        WHEN payment_amount_ghs IS NULL THEN total_item_value_ghs
        ELSE payment_amount_ghs
    END) AS TotalRevenue
FROM
    ProductUsage
WHERE
    payment_method IS NOT NULL AND payment_method != ''
GROUP BY
    payment_method
ORDER BY
    TotalRevenue DESC;