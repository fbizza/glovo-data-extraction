SELECT DISTINCT customer_id
    FROM delta.central_order_descriptors_odp.order_descriptors_v2
    WHERE order_country_code = 'PL'
      AND order_final_status = 'DeliveredStatus'
      AND order_parent_relationship_type IS NULL
      AND order_is_first_delivered_order = true
      AND store_name not IN ('McDonald''s', 'KFC')
      AND order_started_local_at <= DATE_ADD('day', 1, DATE '2024-09-01')
      AND order_started_local_at >= DATE '2024-08-01'