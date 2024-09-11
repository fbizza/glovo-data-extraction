WITH order_data AS (
    SELECT
        o.order_id,
        o.order_total_purchase_eur,
        SUM(p.bought_product_quantity) AS n_items
    FROM delta.central_order_descriptors_odp.order_descriptors_v2 o
    LEFT JOIN delta.customer_bought_products_odp.bought_products_v2 p ON o.order_id = p.order_id
    WHERE o.order_country_code = 'PL'
        AND o.order_final_status = 'DeliveredStatus'
        AND o.order_parent_relationship_type IS NULL
        AND date(o.order_started_local_at) >= date '2024-01-01'
    GROUP BY o.order_id, o.order_total_purchase_eur
),
aov_per_item_data AS (
    SELECT
        order_id,
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
        COUNT(order_id) AS orders,
        ROUND(AVG(aov_per_item), 2) AS aov_per_item
    FROM aov_per_item_data
    GROUP BY 1
)
SELECT
    order_bucket,
    orders,
    aov_per_item,
    ROUND((orders * 100.0 / total_orders), 2) AS percentage_of_total_orders
FROM bucketed_data,
    (SELECT SUM(orders) AS total_orders FROM bucketed_data) AS total;

