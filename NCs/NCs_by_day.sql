    SELECT
        extract (month from o.order_started_local_at) as month,
        COUNT(DISTINCT CASE WHEN o.order_is_first_delivered_order THEN o.customer_id ELSE NULL END) AS NC
    FROM delta.central_order_descriptors_odp.order_descriptors_v2 o
    WHERE o.order_started_local_at BETWEEN date('2024-01-01') AND date('2024-10-01')
        AND o.order_country_code = 'PL'
        AND o.order_final_status = 'DeliveredStatus'
        AND o.order_parent_relationship_type IS NULL
    GROUP BY 1
    ORDER BY 1, 2 ASC






