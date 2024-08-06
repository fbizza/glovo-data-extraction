SELECT date (a.user_created_at) AS date,
	   extract(hour FROM a.user_created_at) AS hour,
       extract(minute FROM a.user_created_at) AS minute,
	   preferred_city_code,
	   count(DISTINCT user_id) AS num_sign_ups
FROM delta.central_users_odp.users_v2 a
LEFT JOIN delta.customer_attributes_odp.customers_v1 b ON a.user_id = b.id
JOIN delta.central_geography_odp.cities_v2 c ON c.city_code = b.preferred_city_code
WHERE user_created_at <= DATE (current_date) AND user_created_at >= DATE ('2024-08-02')
	AND c.country_code = 'PL'
    AND preferred_city_code = 'WAW'
GROUP BY 1, 2, 3, 4
ORDER BY 5 desc


