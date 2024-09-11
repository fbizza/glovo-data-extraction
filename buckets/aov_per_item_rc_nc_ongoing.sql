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
    SELECT (SELECT COUNT() FROM nc) + (SELECT COUNT() FROM rc_categorization) AS total
),
nc_categorization AS (
    SELECT customer_id, 'New Customer' AS customer_status
    FROM nc
),
order_data AS (
    SELECT
        o.order_id,
        o.customer_id,
        o.order_total_purchase_eur,
        SUM(p.bought_product_quantity) AS n_items
    FROM delta.central_order_descriptors_odp.order_descriptors_v2 o
    LEFT JOIN delta.customer_bought_products_odp.bought_products_v2 p ON o.order_id = p.order_id
    WHERE o.order_country_code = 'PL'
        AND o.order_final_status = 'DeliveredStatus'
        AND o.order_parent_relationship_type IS NULL
        AND o.order_started_local_at < DATE '2024-09-01'
        AND o.order_started_local_at >= DATE '2024-08-01'
    GROUP BY o.order_id, o.customer_id, o.order_total_purchase_eur
),
aov_per_item_data AS (
    SELECT
        order_id,
        customer_id,
        order_total_purchase_eur,
        n_items,
        ROUND(order_total_purchase_eur / n_items, 2) AS aov_per_item
    FROM order_data
),
bucketed_data AS (
    SELECT
        CASE
            WHEN aov_per_item < 3 THEN '<3'
            WHEN aov_per_item >= 3 AND aov_per_item < 5 THEN '3-5'
            WHEN aov_per_item >= 5 AND aov_per_item < 7 THEN '5-7'
            WHEN aov_per_item >= 7 AND aov_per_item < 9 THEN '7-9'
            WHEN aov_per_item >= 9 AND aov_per_item < 11 THEN '9-11'
            WHEN aov_per_item >= 11 AND aov_per_item < 13 THEN '11-13'
            ELSE '13+'
        END AS order_bucket,
        customer_id,
        COUNT(order_id) AS orders,
        ROUND(AVG(aov_per_item), 2) AS aov_per_item
    FROM aov_per_item_data
    GROUP BY 1, customer_id
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
        bd.order_bucket,
        bd.customer_id,
        cc.customer_status,
        bd.orders,
        bd.aov_per_item
    FROM bucketed_data bd
    JOIN customer_categorization cc ON bd.customer_id = cc.customer_id
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
    MAX(CASE WHEN order_bucket = '<3' THEN percentage_of_bucket_total ELSE 0 END) AS "<3",
    MAX(CASE WHEN order_bucket = '3-5' THEN percentage_of_bucket_total ELSE 0 END) AS "3-5",
    MAX(CASE WHEN order_bucket = '5-7' THEN percentage_of_bucket_total ELSE 0 END) AS "5-7",
    MAX(CASE WHEN order_bucket = '7-9' THEN percentage_of_bucket_total ELSE 0 END) AS "7-9",
    MAX(CASE WHEN order_bucket = '9-11' THEN percentage_of_bucket_total ELSE 0 END) AS "9-11",
    MAX(CASE WHEN order_bucket = '11-13' THEN percentage_of_bucket_total ELSE 0 END) AS "11-13",
    MAX(CASE WHEN order_bucket = '13+' THEN percentage_of_bucket_total ELSE 0 END) AS "13+"
FROM final_table
GROUP BY customer_status
ORDER BY customer_status;