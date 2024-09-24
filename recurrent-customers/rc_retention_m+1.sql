WITH nc AS (
    SELECT DISTINCT customer_id
    FROM delta.central_order_descriptors_odp.order_descriptors_v2
    WHERE order_country_code = 'PL'
      AND order_city_code = 'WAW'
      AND order_final_status = 'DeliveredStatus'
      AND order_parent_relationship_type IS NULL
      AND order_is_first_delivered_order = true
      AND order_started_local_at < DATE '2024-01-01'
      AND order_started_local_at >= DATE '2023-12-01'
),
rc AS (
    SELECT DISTINCT customer_id
    FROM delta.central_order_descriptors_odp.order_descriptors_v2
    WHERE order_country_code = 'PL'
      AND order_city_code = 'WAW'
      AND order_final_status = 'DeliveredStatus'
      AND order_parent_relationship_type IS NULL
      AND order_started_local_at < DATE '2024-01-01'
      AND order_started_local_at >= DATE '2023-12-01'
    EXCEPT
    SELECT * FROM nc
),
rc_next_month_orders AS (
    SELECT DISTINCT customer_id
    FROM delta.central_order_descriptors_odp.order_descriptors_v2
    WHERE order_country_code = 'PL'
      AND order_city_code = 'WAW'
      AND order_final_status = 'DeliveredStatus'
      AND order_parent_relationship_type IS NULL
      AND order_started_local_at < DATE '2024-02-01'
      AND order_started_local_at >= DATE '2024-01-01'
      AND customer_id IN (SELECT customer_id FROM rc)
)
SELECT
    COUNT(DISTINCT rco.customer_id) AS rc_customers_retained,
    COUNT(DISTINCT rc.customer_id) AS total_rc_customers_previous_month,
    COUNT(DISTINCT rco.customer_id) * 1.00 / COUNT(DISTINCT rc.customer_id) AS share_of_retained_rc_customers
FROM
    rc
LEFT JOIN
    rc_next_month_orders rco ON rc.customer_id = rco.customer_id;