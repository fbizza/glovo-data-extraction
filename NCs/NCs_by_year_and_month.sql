    SELECT
        EXTRACT(YEAR FROM o.order_started_local_at) AS year,
        EXTRACT(MONTH FROM o.order_started_local_at) AS month,
        COUNT(DISTINCT o.order_id) AS number_of_delivered_orders,
        COUNT(DISTINCT CASE WHEN o.order_is_first_delivered_order THEN o.customer_id ELSE NULL END) AS NC
    FROM delta.central_order_descriptors_odp.order_descriptors_v2 o
    WHERE o.order_started_local_at >= date('2022-01-01')
        AND o.order_country_code = 'PL'
        AND o.order_final_status = 'DeliveredStatus'
        AND o.order_parent_relationship_type IS NULL
    GROUP BY 1, 2
    ORDER BY 1, 2 ASC


