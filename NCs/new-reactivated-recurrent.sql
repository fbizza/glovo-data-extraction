WITH nc as (
    SELECT DISTINCT customer_id
    FROM delta.central_order_descriptors_odp.order_descriptors_v2
    WHERE order_country_code = 'PL'
      AND order_final_status = 'DeliveredStatus'
      AND order_parent_relationship_type IS NULL
      AND order_is_first_delivered_order = true
      AND order_started_local_at < DATE '2024-08-01' and order_started_local_at >= DATE '2024-07-01'
),
    rc AS (
    SELECT DISTINCT customer_id
    FROM delta.central_order_descriptors_odp.order_descriptors_v2
    WHERE order_country_code = 'PL'
      AND order_final_status = 'DeliveredStatus'
      AND order_parent_relationship_type IS NULL
      AND order_started_local_at < DATE '2024-08-01' and order_started_local_at >= DATE '2024-07-01'
    EXCEPT
    SELECT * FROM nc
),
    first_monthly_order_date_rc AS (
    SELECT customer_id, date(MIN(order_started_local_at)) AS monthly_first_order_date
    FROM delta.central_order_descriptors_odp.order_descriptors_v2
    WHERE customer_id IN (SELECT customer_id FROM rc)
    AND order_started_local_at < DATE '2024-08-01' and order_started_local_at >= DATE '2024-07-01'
    GROUP BY customer_id
),
    last_order_before_first AS (
    SELECT a.customer_id, date(MAX(b.order_started_local_at)) AS last_order_date
    FROM first_monthly_order_date_rc a
    LEFT JOIN delta.central_order_descriptors_odp.order_descriptors_v2 b ON a.customer_id = b.customer_id
    WHERE b.order_started_local_at < a.monthly_first_order_date
    GROUP BY a.customer_id
),
    rc_categorization AS (
    SELECT
        a.customer_id,
        a.monthly_first_order_date,
        b.last_order_date,
        CASE
            WHEN DATE_DIFF('day', b.last_order_date, a.monthly_first_order_date) <= 28 THEN 'Ongoing'
            WHEN DATE_DIFF('day', b.last_order_date, a.monthly_first_order_date) > 28 THEN 'Reactivated'
        END AS customer_status
    FROM first_monthly_order_date_rc a
    JOIN last_order_before_first b ON a.customer_id = b.customer_id
),
    total_customers AS (
    SELECT (SELECT count(*) from nc) + (SELECT count(*) from rc_categorization)
),
    nc_stats AS (
    SELECT 'New Customers' AS customer_status, COUNT(*) AS total, 1.00000 * COUNT(*) / (SELECT * FROM total_customers) AS percentage
    FROM nc
)
SELECT customer_status, count, percentage
FROM (
    SELECT customer_status, COUNT(customer_id) AS count, 1.00000 * COUNT(customer_id) / (SELECT * FROM total_customers) AS percentage
    FROM rc_categorization
    GROUP BY customer_status
    UNION ALL
    SELECT * FROM nc_stats
)