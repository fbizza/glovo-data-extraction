WITH nc AS (
    SELECT DISTINCT customer_id
    FROM delta.central_order_descriptors_odp.order_descriptors_v2
    WHERE order_country_code = 'PL'
      AND order_final_status = 'DeliveredStatus'
      AND order_parent_relationship_type IS NULL
      AND order_is_first_delivered_order = true
      AND order_started_local_at < DATE '2024-09-01'
      AND order_started_local_at >= DATE '2024-08-01'
),
rc AS (
    SELECT DISTINCT customer_id
    FROM delta.central_order_descriptors_odp.order_descriptors_v2
    WHERE order_country_code = 'PL'
      AND order_final_status = 'DeliveredStatus'
      AND order_parent_relationship_type IS NULL
      AND order_started_local_at < DATE '2024-09-01'
      AND order_started_local_at >= DATE '2024-08-01'
    EXCEPT
    SELECT * FROM nc
),
first_monthly_order_date_rc AS (
    SELECT customer_id, DATE(MIN(order_started_local_at)) AS monthly_first_order_date
    FROM delta.central_order_descriptors_odp.order_descriptors_v2
    WHERE customer_id IN (SELECT customer_id FROM rc)
      AND order_started_local_at < DATE '2024-09-01'
      AND order_started_local_at >= DATE '2024-08-01'
    GROUP BY customer_id
),
last_order_before_first AS (
    SELECT a.customer_id, DATE(MAX(b.order_started_local_at)) AS last_order_date
    FROM first_monthly_order_date_rc a
    LEFT JOIN delta.central_order_descriptors_odp.order_descriptors_v2 b
    ON a.customer_id = b.customer_id
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
    SELECT (SELECT COUNT(*) FROM nc) + (SELECT COUNT(*) FROM rc_categorization) AS total
),
nc_categorization AS (
    SELECT customer_id, 'New Customer' AS customer_status   -- add extra division if needed
    FROM nc
),

order_data AS (
    SELECT
        CASE
            WHEN order_total_purchase_eur < 5 THEN '<5'
            WHEN order_total_purchase_eur >= 5 AND order_total_purchase_eur < 10 THEN '5-10'
            WHEN order_total_purchase_eur >= 10 AND order_total_purchase_eur < 15 THEN '10-15'
            WHEN order_total_purchase_eur >= 15 AND order_total_purchase_eur < 20 THEN '15-20'
            WHEN order_total_purchase_eur >= 20 AND order_total_purchase_eur < 25 THEN '20-25'
            WHEN order_total_purchase_eur >= 25 AND order_total_purchase_eur < 30 THEN '25-30'
            ELSE '30+'
        END AS order_bucket,
        o.customer_id,
        COUNT(order_id) AS orders,
        ROUND(AVG(order_total_purchase_eur), 2) AS aov
    FROM delta.central_order_descriptors_odp.order_descriptors_v2 o
    WHERE o.order_country_code = 'PL'
        AND o.order_started_local_at < DATE '2024-09-01'
        AND o.order_started_local_at >= DATE '2024-08-01'
        AND order_final_status = 'DeliveredStatus'
        AND order_parent_relationship_type IS NULL
    GROUP BY 1, o.customer_id
),
customer_categorization AS (
    SELECT customer_id, customer_status
    FROM rc_categorization
    UNION ALL
    SELECT customer_id, customer_status
    FROM nc_categorization
),
category_counts AS (
    SELECT customer_status, COUNT(customer_id) AS count
    FROM customer_categorization
    GROUP BY customer_status
),
order_with_customer AS (
    SELECT
        od.order_bucket,
        od.customer_id,
        cc.customer_status,
        od.orders,
        od.aov
    FROM order_data od
    JOIN customer_categorization cc ON od.customer_id = cc.customer_id
),
bucket_totals AS (
    SELECT
        order_bucket,
        SUM(orders) AS total_orders
    FROM order_with_customer
    GROUP BY order_bucket
),
customer_group_totals AS (
    SELECT
        order_bucket,
        customer_status,
        SUM(orders) AS customer_group_orders
    FROM order_with_customer
    GROUP BY order_bucket, customer_status
),
final_table AS (
    SELECT
        cgt.customer_status,
        cgt.order_bucket,
        cgt.customer_group_orders,
        ROUND((cgt.customer_group_orders * 100.0 / bt.total_orders), 2) AS percentage_of_bucket_total
    FROM customer_group_totals cgt
    JOIN bucket_totals bt ON cgt.order_bucket = bt.order_bucket
)
SELECT
    customer_status,
    MAX(CASE WHEN order_bucket = '<5' THEN percentage_of_bucket_total ELSE 0 END) AS "<5",
    MAX(CASE WHEN order_bucket = '5-10' THEN percentage_of_bucket_total ELSE 0 END) AS "5-10",
    MAX(CASE WHEN order_bucket = '10-15' THEN percentage_of_bucket_total ELSE 0 END) AS "10-15",
    MAX(CASE WHEN order_bucket = '15-20' THEN percentage_of_bucket_total ELSE 0 END) AS "15-20",
    MAX(CASE WHEN order_bucket = '20-25' THEN percentage_of_bucket_total ELSE 0 END) AS "20-25",
    MAX(CASE WHEN order_bucket = '25-30' THEN percentage_of_bucket_total ELSE 0 END) AS "25-30",
    MAX(CASE WHEN order_bucket = '30+' THEN percentage_of_bucket_total ELSE 0 END) AS "30+"
FROM final_table
GROUP BY customer_status
ORDER BY customer_status;
