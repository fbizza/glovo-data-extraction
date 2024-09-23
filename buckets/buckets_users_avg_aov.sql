WITH customer_avg_aov AS (
    SELECT
        customer_id,
        ROUND(AVG(order_total_purchase_eur), 2) AS avg_aov,
        COUNT(DISTINCT store_name) AS distinct_stores
    FROM delta.central_order_descriptors_odp.order_descriptors_v2 o
    WHERE o.order_country_code = 'PL'
        AND date(order_started_local_at) >= date '2024-01-01'
        AND order_final_status = 'DeliveredStatus'
        AND order_parent_relationship_type IS NULL
    GROUP BY customer_id
),
customer_data AS (
    SELECT
        CASE
            WHEN avg_aov < 5 THEN '<5'
            WHEN avg_aov >= 5 AND avg_aov < 10 THEN '5-10'
            WHEN avg_aov >= 10 AND avg_aov < 15 THEN '10-15'
            WHEN avg_aov >= 15 AND avg_aov < 20 THEN '15-20'
            WHEN avg_aov >= 20 AND avg_aov < 25 THEN '20-25'
            WHEN avg_aov >= 25 AND avg_aov < 30 THEN '25-30'
            ELSE '30+'
        END AS aov_bucket,
        COUNT(customer_id) AS customers,
        ROUND(AVG(avg_aov), 2) AS avg_aov,
        ROUND(AVG(distinct_stores), 2) AS avg_distinct_stores
    FROM customer_avg_aov
    GROUP BY 1
)
SELECT
    aov_bucket,
    customers,
    avg_aov,
    avg_distinct_stores,
    ROUND((customers * 100.0 / total_customers), 2) AS percentage_of_total_customers
FROM customer_data,
    (SELECT SUM(customers) AS total_customers FROM customer_data) AS total