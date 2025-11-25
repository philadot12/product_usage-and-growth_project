SELECT
    product_id,
    product_name,
    category,
    brand,
    COUNT(DISTINCT order_id) AS OrdersCount,
    SUM(quantity) AS TotalUnitsSold,
    SUM(total_item_value_ghs) AS ProductRevenue
FROM
    ProductUsage
GROUP BY
    product_id, product_name, category, brand
ORDER BY
    ProductRevenue DESC
LIMIT 10;

-- Revenue by Category for Visualization
SELECT
    category,
    SUM(CASE
        WHEN payment_amount_ghs IS NULL THEN total_item_value_ghs
        ELSE payment_amount_ghs
    END) AS CategoryRevenue
FROM
    ProductUsage
GROUP BY
    category
ORDER BY
    CategoryRevenue DESC;
