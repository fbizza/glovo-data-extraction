WITH order_data AS (
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
        COUNT(order_id) AS orders,
        ROUND(AVG(order_total_purchase_eur), 2) AS aov
    FROM delta.central_order_descriptors_odp.order_descriptors_v2 o
    WHERE o.order_country_code = 'PL'
        AND date(order_started_local_at) >= date '2024-08-01'
        AND order_final_status = 'DeliveredStatus'
        AND order_parent_relationship_type IS NULL
    GROUP BY 1
)
SELECT
    order_bucket,
    orders,
    aov,
    ROUND((orders * 100.0 / total_orders), 2) AS percentage_of_total_orders
FROM order_data,
    (SELECT SUM(orders) AS total_orders FROM order_data) AS total