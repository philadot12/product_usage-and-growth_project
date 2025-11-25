SELECT
    order_id,
    user_id,
    SUM(total_item_value_ghs) AS calculated_order_value,
    payment_amount_ghs
FROM
    ProductUsage
WHERE
    payment_amount_ghs IS NULL OR payment_amount_ghs = 0
GROUP BY 1, 2, 4
LIMIT 10;
-- This query shows which orders have NULL payment amounts but recorded item values.