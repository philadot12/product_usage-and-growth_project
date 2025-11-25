-- Monthly Active Users (MAU) - based on order activity
SELECT
    order_month AS activity_month,
    COUNT(DISTINCT user_id) AS Monthly_Active_Users
FROM
    ProductUsage
GROUP BY
    activity_month
ORDER BY
    activity_month;