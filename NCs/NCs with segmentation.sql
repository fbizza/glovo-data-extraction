WITH nc_by_store_with_year_and_month
AS (
	SELECT o.store_name,
		Extract(year FROM o.order_started_local_at) AS YEAR,
		Extract(month FROM o.order_started_local_at) AS MONTH,
		Count(DISTINCT order_id) AS number_of_orders,
		Count(CASE WHEN order_is_first_delivered_order = true THEN customer_id ELSE NULL END) AS new_customers
	FROM delta.central_order_descriptors_odp.order_descriptors_v2 o
	WHERE o.order_started_local_at >= DATE ('2024-01-01')
		AND o.order_country_code = 'PL'
		AND o.order_final_status = 'DeliveredStatus'
		AND o.order_parent_relationship_type IS NULL
	GROUP BY o.store_name, 2, 3
	ORDER BY 2, 3 ASC, 4 DESC
	)
SELECT s.store_name,
	number_of_orders,
	new_customers
FROM nc_by_store_with_year_and_month nc
LEFT JOIN delta.partner_segmentation_odp.daily_partner_segmentation s ON nc.store_name = s.store_name
GROUP BY s.store_name,
	number_of_orders,
	new_customers
ORDER BY 3 DESC