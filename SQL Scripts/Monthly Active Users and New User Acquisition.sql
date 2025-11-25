    
    -- New User Acquisition (Identifying the first order date for each user)
WITH UserFirstOrder AS (
    SELECT
        user_id,
        MIN(STR_TO_DATE(order_date, '%m/%d/%Y %H:%i')) AS FirstOrderDate
    FROM
        ProductUsage
    GROUP BY
        user_id
)
SELECT
    -- Displays the month in words (e.g., 'August 2025')
    DATE_FORMAT(FirstOrderDate, '%M %Y') AS AcquisitionMonth,
    COUNT(user_id) AS NewUsers
FROM
    UserFirstOrder
GROUP BY
    AcquisitionMonth,
    DATE_FORMAT(FirstOrderDate, '%Y-%m')
ORDER BY
    DATE_FORMAT(FirstOrderDate, '%Y-%m');
