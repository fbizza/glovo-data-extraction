WITH order_data AS (
    SELECT
        o.order_id,
        o.order_total_purchase_eur,
        SUM(p.bought_product_quantity) AS n_items,
        o.order_vertical
    FROM delta.central_order_descriptors_odp.order_descriptors_v2 o
    LEFT JOIN delta.customer_bought_products_odp.bought_products_v2 p ON o.order_id = p.order_id
    WHERE o.order_country_code = 'PL'
        AND o.order_final_status = 'DeliveredStatus'
        AND o.order_parent_relationship_type IS NULL
        AND date(o.order_started_local_at) >= date '2024-01-01'
    GROUP BY o.order_id, o.order_total_purchase_eur, o.order_vertical
),
aov_per_item_data AS (
    SELECT
        order_id,
        order_total_purchase_eur,
        n_items,
        order_vertical,
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
        COUNT(order_id) AS orders,
        ROUND(AVG(aov_per_item), 2) AS aov_per_item,
        SUM(CASE WHEN order_vertical = 'Food' THEN 1 ELSE 0 END) AS food_orders,
        SUM(CASE WHEN order_vertical = 'QCommerce' THEN 1 ELSE 0 END) AS qcommerce_orders
    FROM aov_per_item_data
    GROUP BY 1
),
total_orders AS (
    SELECT SUM(orders) AS total_orders FROM bucketed_data
)
SELECT
    'orders' AS metric,
    SUM(CASE WHEN order_bucket = '<3' THEN orders ELSE 0 END) AS "<3",
    SUM(CASE WHEN order_bucket = '3-5' THEN orders ELSE 0 END) AS "3-5",
    SUM(CASE WHEN order_bucket = '5-7' THEN orders ELSE 0 END) AS "5-7",
    SUM(CASE WHEN order_bucket = '7-9' THEN orders ELSE 0 END) AS "7-9",
    SUM(CASE WHEN order_bucket = '9-11' THEN orders ELSE 0 END) AS "9-11",
    SUM(CASE WHEN order_bucket = '11-13' THEN orders ELSE 0 END) AS "11-13",
    SUM(CASE WHEN order_bucket = '13+' THEN orders ELSE 0 END) AS "13+"
FROM bucketed_data
UNION ALL
SELECT
    'avg_aov_per_item' AS metric,
    SUM(CASE WHEN order_bucket = '<3' THEN aov_per_item ELSE 0 END) AS "<3",
    SUM(CASE WHEN order_bucket = '3-5' THEN aov_per_item ELSE 0 END) AS "3-5",
    SUM(CASE WHEN order_bucket = '5-7' THEN aov_per_item ELSE 0 END) AS "5-7",
    SUM(CASE WHEN order_bucket = '7-9' THEN aov_per_item ELSE 0 END) AS "7-9",
    SUM(CASE WHEN order_bucket = '9-11' THEN aov_per_item ELSE 0 END) AS "9-11",
    SUM(CASE WHEN order_bucket = '11-13' THEN aov_per_item ELSE 0 END) AS "11-13",
    SUM(CASE WHEN order_bucket = '13+' THEN aov_per_item ELSE 0 END) AS "13+"
FROM bucketed_data
UNION ALL
SELECT
    'percentage_of_total_orders' AS metric,
    SUM(CASE WHEN order_bucket = '<3' THEN ROUND((orders * 100.0 / total_orders.total_orders), 2) ELSE 0 END) AS "<3",
    SUM(CASE WHEN order_bucket = '3-5' THEN ROUND((orders * 100.0 / total_orders.total_orders), 2) ELSE 0 END) AS "3-5",
    SUM(CASE WHEN order_bucket = '5-7' THEN ROUND((orders * 100.0 / total_orders.total_orders), 2) ELSE 0 END) AS "5-7",
    SUM(CASE WHEN order_bucket = '7-9' THEN ROUND((orders * 100.0 / total_orders.total_orders), 2) ELSE 0 END) AS "7-9",
    SUM(CASE WHEN order_bucket = '9-11' THEN ROUND((orders * 100.0 / total_orders.total_orders), 2) ELSE 0 END) AS "9-11",
    SUM(CASE WHEN order_bucket = '11-13' THEN ROUND((orders * 100.0 / total_orders.total_orders), 2) ELSE 0 END) AS "11-13",
    SUM(CASE WHEN order_bucket = '13+' THEN ROUND((orders * 100.0 / total_orders.total_orders), 2) ELSE 0 END) AS "13+"
FROM bucketed_data, total_orders
UNION ALL
SELECT
    'percentage_food_orders' AS metric,
    SUM(CASE WHEN order_bucket = '<3' THEN ROUND((food_orders * 100.0 / orders), 2) ELSE 0 END) AS "<3",
    SUM(CASE WHEN order_bucket = '3-5' THEN ROUND((food_orders * 100.0 / orders), 2) ELSE 0 END) AS "3-5",
    SUM(CASE WHEN order_bucket = '5-7' THEN ROUND((food_orders * 100.0 / orders), 2) ELSE 0 END) AS "5-7",
    SUM(CASE WHEN order_bucket = '7-9' THEN ROUND((food_orders * 100.0 / orders), 2) ELSE 0 END) AS "7-9",
    SUM(CASE WHEN order_bucket = '9-11' THEN ROUND((food_orders * 100.0 / orders), 2) ELSE 0 END) AS "9-11",
    SUM(CASE WHEN order_bucket = '11-13' THEN ROUND((food_orders * 100.0 / orders), 2) ELSE 0 END) AS "11-13",
    SUM(CASE WHEN order_bucket = '13+' THEN ROUND((food_orders * 100.0 / orders), 2) ELSE 0 END) AS "13+"
FROM bucketed_data, total_orders
UNION ALL
SELECT
    'percentage_qcommerce_orders' AS metric,
    SUM(CASE WHEN order_bucket = '<3' THEN ROUND((qcommerce_orders * 100.0 / orders), 2) ELSE 0 END) AS "<3",
    SUM(CASE WHEN order_bucket = '3-5' THEN ROUND((qcommerce_orders * 100.0 / orders), 2) ELSE 0 END) AS "3-5",
    SUM(CASE WHEN order_bucket = '5-7' THEN ROUND((qcommerce_orders * 100.0 / orders), 2) ELSE 0 END) AS "5-7",
    SUM(CASE WHEN order_bucket = '7-9' THEN ROUND((qcommerce_orders * 100.0 / orders), 2) ELSE 0 END) AS "7-9",
    SUM(CASE WHEN order_bucket = '9-11' THEN ROUND((qcommerce_orders * 100.0 / orders), 2) ELSE 0 END) AS "9-11",
    SUM(CASE WHEN order_bucket = '11-13' THEN ROUND((qcommerce_orders * 100.0 / orders), 2) ELSE 0 END) AS "11-13",
    SUM(CASE WHEN order_bucket = '13+' THEN ROUND((qcommerce_orders * 100.0 / orders), 2) ELSE 0 END) AS "13+"
FROM bucketed_data, total_orders;