SELECT
    date(date_trunc('week', order_started_local_at)) d_month,
    count(distinct o.customer_id) AS new_customers
FROM delta.central_order_descriptors_odp.order_descriptors_v2 o
WHERE o.order_country_code = 'PL'
    AND o.order_final_status = 'DeliveredStatus'
    AND o.order_parent_relationship_type IS NULL
    AND date(date_trunc('week', order_started_local_at)) > date '2024-01-01'
    AND o.order_is_first_delivered_order
   -- AND o.store_name = 'McDonald''s'
GROUP BY 1
order by 1 asc






