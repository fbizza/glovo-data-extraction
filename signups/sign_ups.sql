SELECT date(DATE_TRUNC('week', a.user_created_at)) AS week,
       c.country_code,
       count(distinct user_id) AS num_su
FROM delta.central_users_odp.users_v2 a
         LEFT JOIN delta.customer_attributes_odp.customers_v1 b on a.user_id = b.id
         LEFT JOIN delta.central_geography_odp.cities_v2 c on c.city_code = b.preferred_city_code
WHERE user_created_at < date '2024-09-30'
AND c.country_code in ('PL', 'ES', 'RO', 'IT', 'PT', 'HR', 'RS')
GROUP BY 1, 2
order by 1 desc, 2