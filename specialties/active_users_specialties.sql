WITH specialties_stores
AS (
	SELECT store_id
	FROM delta.partner_stores_odp.stores_v2 AS stores_current
	WHERE (
			stores_current.store_vertical = 'QCommerce'
			AND stores_current.store_subvertical2 = 'Groceries'
			AND stores_current.store_sub_business_unit NOT IN (
				'Convenience',
				'Supermarket',
				'Other',
				'Fake',
				'CONVENIENCE',
				'OTHER',
				'SUPERMARKET',
				'FAKE'
				)
			)
	)
SELECT
    DATE (date_trunc('month', o.order_started_local_at)) AS d_month,
	count(DISTINCT customer_id) AS active_users
FROM delta.central_order_descriptors_odp.order_descriptors_v2 o
WHERE o.order_country_code = 'PL'
	AND o.order_final_status = 'DeliveredStatus'
	AND o.order_parent_relationship_type IS NULL
	AND o.order_started_local_at > DATE '2024-01-01'
    AND o.store_id in (select * from specialties_stores)
GROUP BY 1
ORDER BY 1 ASC