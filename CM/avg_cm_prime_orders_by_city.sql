SELECT date (date_trunc('month', o.order_started_local_at)) AS d_month,
    o.order_city_code as city,
    avg (contribution_margin_eur) as avg_CM_eur,
    avg (cm0pnl_eur) as avg_cm0pnl_eur,
    count (distinct o.order_id) as num_orders
FROM delta.central_order_descriptors_odp.order_descriptors_v2 o LEFT JOIN delta.finance_financial_reports_odp.pnl_order_level f
ON o.order_id = f.order_id
WHERE o.order_country_code = 'PL'
  AND o.order_is_prime = false
  AND o.order_started_local_at >= date '2024-06-01'
  AND o.order_started_local_at < date '2024-07-01'
  AND o.order_final_status = 'DeliveredStatus'
  AND o.order_parent_relationship_type is null
GROUP BY 1, 2
ORDER BY 5 desc