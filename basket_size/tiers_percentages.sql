
    SELECT
        date (date_trunc('day', MIN(o.order_started_local_at))) AS start_day,
        date (date_trunc('day', MAX(o.order_started_local_at))) AS end_day,
        COUNT(DISTINCT o.order_id) AS total_orders,
        COUNT(DISTINCT CASE WHEN order_total_purchase_local < 40 THEN o.order_id ELSE NULL END) AS less_than_40,
        COUNT(DISTINCT CASE WHEN order_total_purchase_local >= 40 AND order_total_purchase_local < 50 THEN o.order_id ELSE NULL END) AS in_40_50,
        COUNT(DISTINCT CASE WHEN order_total_purchase_local >= 50 AND order_total_purchase_local < 60 THEN o.order_id ELSE NULL END) AS in_50_60,
        COUNT(DISTINCT CASE WHEN order_total_purchase_local >= 60 THEN o.order_id ELSE NULL END) AS more_than_60
    FROM
        delta.central_order_descriptors_odp.order_descriptors_v2 o
    WHERE
        order_country_code = 'PL'
        AND o.order_final_status = 'DeliveredStatus'
        AND o.order_parent_relationship_type IS NULL
        AND store_name = 'McDonald''s'
        AND date_trunc('day', o.order_started_local_at) >= date '2024-07-01'  -- included
        AND date_trunc('day', o.order_started_local_at) <= date '2024-08-01'  -- excluded

