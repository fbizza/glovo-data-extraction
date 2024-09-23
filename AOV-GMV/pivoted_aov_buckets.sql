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
        1.00 * COUNT(DISTINCT store_name) AS n_distinct_partners,
        ROUND(AVG(order_total_purchase_eur), 2) AS avg_aov,
        SUM(CASE WHEN order_vertical = 'Food' THEN 1 ELSE 0 END) AS food_orders,
        SUM(CASE WHEN order_vertical = 'QCommerce' THEN 1 ELSE 0 END) AS qcommerce_orders
    FROM delta.central_order_descriptors_odp.order_descriptors_v2 o
    WHERE o.order_country_code = 'PL'
        AND DATE(order_started_local_at) >= DATE '2024-01-01'
        AND order_final_status = 'DeliveredStatus'
        AND order_parent_relationship_type IS NULL
    GROUP BY 1
),
total_orders AS (
    SELECT SUM(orders) AS total_orders FROM order_data
)
SELECT
    'orders' AS metric,
    SUM(CASE WHEN order_bucket = '<5' THEN orders ELSE 0 END) AS "<5",
    SUM(CASE WHEN order_bucket = '5-10' THEN orders ELSE 0 END) AS "5-10",
    SUM(CASE WHEN order_bucket = '10-15' THEN orders ELSE 0 END) AS "10-15",
    SUM(CASE WHEN order_bucket = '15-20' THEN orders ELSE 0 END) AS "15-20",
    SUM(CASE WHEN order_bucket = '20-25' THEN orders ELSE 0 END) AS "20-25",
    SUM(CASE WHEN order_bucket = '25-30' THEN orders ELSE 0 END) AS "25-30",
    SUM(CASE WHEN order_bucket = '30+' THEN orders ELSE 0 END) AS "30+"
FROM order_data
UNION ALL
SELECT
    'avg_aov' AS metric,
    SUM(CASE WHEN order_bucket = '<5' THEN avg_aov ELSE 0 END) AS "<5",
    SUM(CASE WHEN order_bucket = '5-10' THEN avg_aov ELSE 0 END) AS "5-10",
    SUM(CASE WHEN order_bucket = '10-15' THEN avg_aov ELSE 0 END) AS "10-15",
    SUM(CASE WHEN order_bucket = '15-20' THEN avg_aov ELSE 0 END) AS "15-20",
    SUM(CASE WHEN order_bucket = '20-25' THEN avg_aov ELSE 0 END) AS "20-25",
    SUM(CASE WHEN order_bucket = '25-30' THEN avg_aov ELSE 0 END) AS "25-30",
    SUM(CASE WHEN order_bucket = '30+' THEN avg_aov ELSE 0 END) AS "30+"
FROM order_data
UNION ALL
SELECT
    'n_distinct_partners' AS metric,
    SUM(CASE WHEN order_bucket = '<5' THEN n_distinct_partners ELSE 0 END) AS "<5",
    SUM(CASE WHEN order_bucket = '5-10' THEN n_distinct_partners ELSE 0 END) AS "5-10",
    SUM(CASE WHEN order_bucket = '10-15' THEN n_distinct_partners ELSE 0 END) AS "10-15",
    SUM(CASE WHEN order_bucket = '15-20' THEN n_distinct_partners ELSE 0 END) AS "15-20",
    SUM(CASE WHEN order_bucket = '20-25' THEN n_distinct_partners ELSE 0 END) AS "20-25",
    SUM(CASE WHEN order_bucket = '25-30' THEN n_distinct_partners ELSE 0 END) AS "25-30",
    SUM(CASE WHEN order_bucket = '30+' THEN n_distinct_partners ELSE 0 END) AS "30+"
FROM order_data
UNION ALL
SELECT
    'avg_n_order_per_partner' AS metric,
    SUM(CASE WHEN order_bucket = '<5' THEN 1.00 * orders / n_distinct_partners ELSE 0 END) AS "<5",
    SUM(CASE WHEN order_bucket = '5-10' THEN 1.00 * orders / n_distinct_partners ELSE 0 END) AS "5-10",
    SUM(CASE WHEN order_bucket = '10-15' THEN 1.00 * orders / n_distinct_partners ELSE 0 END) AS "10-15",
    SUM(CASE WHEN order_bucket = '15-20' THEN 1.00 * orders / n_distinct_partners ELSE 0 END) AS "15-20",
    SUM(CASE WHEN order_bucket = '20-25' THEN 1.00 * orders / n_distinct_partners ELSE 0 END) AS "20-25",
    SUM(CASE WHEN order_bucket = '25-30' THEN 1.00 * orders / n_distinct_partners ELSE 0 END) AS "25-30",
    SUM(CASE WHEN order_bucket = '30+' THEN 1.00 * orders / n_distinct_partners ELSE 0 END) AS "30+"
FROM order_data
UNION ALL
SELECT
    'percentage_of_total_orders' AS metric,
    SUM(CASE WHEN order_bucket = '<5' THEN ROUND((orders * 100.0 / total_orders.total_orders), 2) ELSE 0 END) AS "<5",
    SUM(CASE WHEN order_bucket = '5-10' THEN ROUND((orders * 100.0 / total_orders.total_orders), 2) ELSE 0 END) AS "5-10",
    SUM(CASE WHEN order_bucket = '10-15' THEN ROUND((orders * 100.0 / total_orders.total_orders), 2) ELSE 0 END) AS "10-15",
    SUM(CASE WHEN order_bucket = '15-20' THEN ROUND((orders * 100.0 / total_orders.total_orders), 2) ELSE 0 END) AS "15-20",
    SUM(CASE WHEN order_bucket = '20-25' THEN ROUND((orders * 100.0 / total_orders.total_orders), 2) ELSE 0 END) AS "20-25",
    SUM(CASE WHEN order_bucket = '25-30' THEN ROUND((orders * 100.0 / total_orders.total_orders), 2) ELSE 0 END) AS "25-30",
    SUM(CASE WHEN order_bucket = '30+' THEN ROUND((orders * 100.0 / total_orders.total_orders), 2) ELSE 0 END) AS "30+"
FROM order_data, total_orders
UNION ALL
SELECT
    'percentage_food_orders' AS metric,
    SUM(CASE WHEN order_bucket = '<5' THEN ROUND((food_orders * 100.0 / orders), 2) ELSE 0 END) AS "<5",
    SUM(CASE WHEN order_bucket = '5-10' THEN ROUND((food_orders * 100.0 / orders), 2) ELSE 0 END) AS "5-10",
    SUM(CASE WHEN order_bucket = '10-15' THEN ROUND((food_orders * 100.0 / orders), 2) ELSE 0 END) AS "10-15",
    SUM(CASE WHEN order_bucket = '15-20' THEN ROUND((food_orders * 100.0 / orders), 2) ELSE 0 END) AS "15-20",
    SUM(CASE WHEN order_bucket = '20-25' THEN ROUND((food_orders * 100.0 / orders), 2) ELSE 0 END) AS "20-25",
    SUM(CASE WHEN order_bucket = '25-30' THEN ROUND((food_orders * 100.0 / orders), 2) ELSE 0 END) AS "25-30",
    SUM(CASE WHEN order_bucket = '30+' THEN ROUND((food_orders * 100.0 / orders), 2) ELSE 0 END) AS "30+"
FROM order_data, total_orders
UNION ALL
SELECT
    'percentage_qcommerce_orders' AS metric,
    SUM(CASE WHEN order_bucket = '<5' THEN ROUND((qcommerce_orders * 100.0 / orders), 2) ELSE 0 END) AS "<5",
    SUM(CASE WHEN order_bucket = '5-10' THEN ROUND((qcommerce_orders * 100.0 / orders), 2) ELSE 0 END) AS "5-10",
    SUM(CASE WHEN order_bucket = '10-15' THEN ROUND((qcommerce_orders * 100.0 / orders), 2) ELSE 0 END) AS "10-15",
    SUM(CASE WHEN order_bucket = '15-20' THEN ROUND((qcommerce_orders * 100.0 / orders), 2) ELSE 0 END) AS "15-20",
    SUM(CASE WHEN order_bucket = '20-25' THEN ROUND((qcommerce_orders * 100.0 / orders), 2) ELSE 0 END) AS "20-25",
    SUM(CASE WHEN order_bucket = '25-30' THEN ROUND((qcommerce_orders * 100.0 / orders), 2) ELSE 0 END) AS "25-30",
    SUM(CASE WHEN order_bucket = '30+' THEN ROUND((qcommerce_orders * 100.0 / orders), 2) ELSE 0 END) AS "30+"
FROM order_data;