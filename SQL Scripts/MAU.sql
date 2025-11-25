-- Monthly Active Users (MAU) - based on order activity
SELECT
    DATE_FORMAT(STR_TO_DATE(order_date, '%m/%d/%Y %H:%i'), '%Y-%m') AS activity_month,
    COUNT(DISTINCT user_id) AS MAU
FROM
    ProductUsage
GROUP BY
    activity_month
ORDER BY
    activity_month;