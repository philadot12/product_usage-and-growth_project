-- Group users based on whether they had any views AND an order in the dataset
SELECT
    CASE
        WHEN views_count > 0 AND COUNT(DISTINCT order_id) > 0 THEN 'Viewer & Purchaser'
        WHEN views_count > 0 AND COUNT(DISTINCT order_id) = 0 THEN 'Viewer Only'
        ELSE 'Purchaser Only' -- This category needs careful interpretation (maybe they purchased without viewing that specific product row's view count)
    END AS UserSegment,
    COUNT(DISTINCT user_id) AS UserCount,
    SUM(CASE
        WHEN payment_amount_ghs IS NULL THEN total_item_value_ghs
        ELSE payment_amount_ghs
    END) AS TotalRevenue
FROM
    ProductUsage
GROUP BY
    UserSegment;

-- Average Views per Purchased Product
SELECT
    AVG(views_count) AS AverageViewsBeforePurchase,
    COUNT(DISTINCT product_id) AS TotalPurchasedProducts
FROM
 ProductUsage
WHERE
    views_count > 0;