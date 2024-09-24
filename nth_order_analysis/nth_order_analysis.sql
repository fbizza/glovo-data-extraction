SELECT
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_started_local_at) AS order_number,
    o.*
FROM
    delta.central_order_descriptors_odp.order_descriptors_v2 o
WHERE o.order_country_code = 'PL'
    AND o.order_final_status = 'DeliveredStatus'
    AND o.order_parent_relationship_type IS NULL
