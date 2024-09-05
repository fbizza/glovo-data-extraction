SELECT
  o.order_city_code as city,
  c.city_name,
  COUNT(distinct o.order_id) AS orders,
  round(sum(gmvalu_eur), 0) as gmv_eur
FROM
  (delta.central_order_descriptors_odp.order_descriptors_v2 o left join central_geography_odp.cities_v2 c on o.order_city_code = c.city_code)
left join delta.finance_financial_reports_odp.pnl_order_level f on o.order_id = f.order_id
WHERE
  o.order_parent_relationship_type IS NULL
  AND date (date_trunc('month', o.order_started_local_at)) = date '2024-08-01'
  AND o.order_final_status = 'DeliveredStatus'
  AND o.order_country_code = 'PL'
group by 1, 2
order by 3 desc
