SELECT date(DATE_TRUNC('month', a.user_created_at)) AS d_month,
       count(distinct user_id) AS num_su
FROM delta.central_users_odp.users_v2 a
         LEFT JOIN delta.customer_attributes_odp.customers_v1 b on a.user_id = b.id
         LEFT JOIN delta.central_geography_odp.cities_v2 c on c.city_code = b.preferred_city_code
WHERE user_created_at < current_date
AND c.country_code = 'PL'
GROUP BY 1