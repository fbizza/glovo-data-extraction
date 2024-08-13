WITH ranked_stores AS (
    SELECT
        o.store_name,
        EXTRACT(year FROM o.order_started_local_at) AS year,
        EXTRACT(month FROM o.order_started_local_at) AS month,
        COUNT(CASE WHEN order_is_first_delivered_order = true THEN customer_id ELSE NULL END) AS new_customers,
        count (distinct o.order_id) as num_orders,
        RANK() OVER (PARTITION BY EXTRACT(year FROM o.order_started_local_at), EXTRACT(month FROM o.order_started_local_at) ORDER BY COUNT(CASE WHEN order_is_first_delivered_order = true THEN customer_id ELSE NULL END) DESC) AS rank
    FROM
        delta.central_order_descriptors_odp.order_descriptors_v2 o
    WHERE
        o.order_started_local_at >= DATE('2024-07-01') AND o.order_started_local_at <= DATE('2024-08-01') AND
        o.order_country_code = 'PL' AND
        o.order_final_status = 'DeliveredStatus' AND
        o.order_parent_relationship_type IS NULL
    GROUP BY
        o.store_name,
        EXTRACT(year FROM o.order_started_local_at),
        EXTRACT(month FROM o.order_started_local_at)
)
SELECT
    store_name,
    year,
    month,
    new_customers,
    num_orders,
    rank
FROM
    ranked_stores
WHERE
    rank < 5   --change here for n-th best store in terms of new customers
ORDER BY 2, 3
