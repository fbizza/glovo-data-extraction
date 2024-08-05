SELECT extract(year FROM a.user_created_at) AS year,
	   extract(week FROM a.user_created_at) AS week,
	   count(DISTINCT user_id) AS num_sign_ups
FROM delta.central_users_odp.users_v2 a
LEFT JOIN delta.customer_attributes_odp.customers_v1 b ON a.user_id = b.id
JOIN delta.central_geography_odp.cities_v2 c ON c.city_code = b.preferred_city_code
WHERE user_created_at <= DATE (current_date) AND user_created_at >= DATE ('2024-05-17')
	AND c.country_code = 'PL'
GROUP BY 1, 2
ORDER BY 1, 2 ASC


