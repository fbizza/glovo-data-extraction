SELECT date (date_trunc('month', order_started_local_at)) AS d_month, order_city_code as city, avg(rpo_local_currency) as avg_total_revenue, count (distinct
order_id) as num_orders
FROM delta.finance_financial_metrics_odp.financial_order_metrics f
WHERE order_country_code = 'PL'
  AND f.order_is_prime = false
  AND order_started_local_at >= date '2024-06-01' and order_started_local_at < date '2024-07-01'
  AND order_final_status = 'DeliveredStatus'
  AND order_parent_relationship_type is null
GROUP BY 1, 2
ORDER BY 4 desc


