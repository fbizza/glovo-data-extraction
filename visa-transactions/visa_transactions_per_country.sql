SELECT
  card_provider,
  date(DATE_TRUNC('month', o.order_started_local_at)),
  COUNT(CASE WHEN o.order_country_code = 'PL' THEN o.order_id END) AS transactions_PL,
  COUNT(CASE WHEN o.order_country_code = 'ES' THEN o.order_id END) AS transactions_ES,
  COUNT(CASE WHEN o.order_country_code = 'IT' THEN o.order_id END) AS transactions_IT,
  COUNT(CASE WHEN o.order_country_code = 'PT' THEN o.order_id END) AS transactions_PT
FROM
  delta.central_order_descriptors_odp.order_descriptors_v2 o
LEFT JOIN
  delta.fintech_payments_odp.payments_v2 p
ON
  o.order_id = p.order_id
WHERE
  o.order_country_code IN ('PL', 'ES', 'IT', 'PT')
  AND o.order_parent_relationship_type IS NULL
  AND o.order_started_local_at >= DATE('2022-01-01')
  AND card_provider = 'Visa'
  AND order_final_status = 'DeliveredStatus'
GROUP BY
  1, 2
ORDER BY
  2 asc