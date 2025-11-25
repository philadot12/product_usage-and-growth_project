SELECT
   order_month,
    COUNT(DISTINCT order_id) AS TotalTransactions,
    SUM(CASE
        WHEN payment_amount_ghs IS NULL THEN total_item_value_ghs  -- Use item value if payment is missing
        ELSE payment_amount_ghs
    END) AS TotalRevenueGHS,
    -- AOV = Total Revenue / Total Transactions
    ROUND(SUM(CASE
        WHEN payment_amount_ghs IS NULL THEN total_item_value_ghs
        ELSE payment_amount_ghs
    END) / COUNT(DISTINCT order_id), 2) AS AOV_GHS
FROM
    ProductUsage
GROUP BY
  order_month
ORDER BY
	TotalRevenueGHS Desc;
  