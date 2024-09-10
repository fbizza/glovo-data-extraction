SELECT date (date_trunc('day', p_event_date)) AS day , count(*) as installs
FROM delta.growth_adjust_odp.adjust_install_v3
WHERE app_install_country = 'PL'
  AND app_install_event_time >= date ('2024-08-19')
GROUP BY 1
ORDER BY 1 ASC

SELECT date (a.user_created_at) AS date,
	   count(DISTINCT user_id) AS num_sign_ups
FROM delta.central_users_odp.users_v2 a
LEFT JOIN delta.customer_attributes_odp.customers_v1 b ON a.user_id = b.id
JOIN delta.central_geography_odp.cities_v2 c ON c.city_code = b.preferred_city_code
WHERE user_created_at >= DATE ('2024-08-01')
	AND c.country_code = 'PL'
--     AND preferred_city_code = 'WAW'
GROUP BY 1
order by 1 asc

SELECT
    date(date_trunc('day', order_started_local_at)) day,
    count(distinct o.customer_id) AS new_customers
FROM delta.central_order_descriptors_odp.order_descriptors_v2 o
WHERE o.order_country_code = 'PL'
    AND o.order_final_status = 'DeliveredStatus'
    AND o.order_parent_relationship_type IS NULL
    AND date(order_started_local_at) > date '2024-08-01'
    AND o.order_is_first_delivered_order
GROUP BY 1
order by 1 asc
