WITH info_period AS (SELECT distinct DATE_TRUNC('month', a.order_activated_local_at) AS month_order_activated_local_at
                                   , a.order_country_code
                                   , a.order_id
                                   , a.customer_id
                                   , a.order_is_first_delivered_order
                                   , case
                                         when (
                                                     a.order_country_Code in ('PL')
                                                 and store_name in ('McDonald''s')
                                             ) then true
                                         else false
        end                                                                          as mcd_cities
                                   , a.order_is_prime
                                   , a.order_activated_local_at
                     FROM delta.central_order_descriptors_odp.order_descriptors_v2 a
                     WHERE a.order_final_status = 'DeliveredStatus'
                       AND a.order_parent_relationship_type is NULL
                       AND a.order_subvertical <> 'On-Demand'
                       AND order_country_Code IN
                           ('ES', 'AD', 'PT', 'PL', 'IT', 'RO', 'MD', 'HR', 'RS', 'ME', 'BA', 'BG', 'SI', 'UA', 'GE',
                            'KZ', 'KG', 'AM', 'CI', 'KE', 'MA', 'TN', 'NG', 'UG', 'GH')
                       and order_activated_local_at < current_date)
   , info_period_rank as (select *,
                                 row_number() over (partition by customer_id , month_order_activated_local_at order by order_activated_local_at) as rank_month
                          from info_period)

   , info_users_period AS (SELECT DATE_TRUNC('month', a.user_created_at)       AS users_registration_month
                                , a.user_id
                                , at_timezone(user_created_at, city_time_zone) as registration_date_local
                                , user_created_at                              as registration_date
                                , preferred_city_code
                                , country_code
                                , user_is_deleted
                           FROM delta.central_users_odp.users_v2 a
                                    LEFT JOIN delta.customer_attributes_odp.customers_v1 b on a.user_id = b.id
                                    LEFT JOIN delta.central_geography_odp.cities_v2 c
                                              on c.city_code = b.preferred_city_code
                           WHERE user_created_at < current_date)
   , number_downloads AS (SELECT DATE_TRUNC('month', app_install_created_at) AS download_time
                               , app_install_country                         AS countryd
                               , count(*)                                    AS downloads_count
                          FROM delta.growth_adjust_odp.adjust_install_v3
                          WHERE app_install_created_at < current_date
                          GROUP BY 1, 2)
   , number_signups AS (SELECT DATE_TRUNC('month', a.registration_date) AS users_registration_month
                             , b.country_code                           AS countryns
                             , COUNT(DISTINCT a.user_id)                AS users_count
                        FROM info_users_period a
                                 LEFT JOIN delta.central_geography_odp.cities_v2 b
                                           on a.preferred_city_code = b.city_code
                        WHERE a.user_is_deleted = false
                          and a.registration_date < current_date
                        GROUP BY 1, 2)
   , active_customers AS (SELECT month_order_activated_local_at as monthac
                               , order_country_code             AS countryac
                               , COUNT(DISTINCT customer_id)    AS number_of_active_customers
                          FROM info_period
                          GROUP BY 1, 2)
   , newnew_newrecurrent AS (SELECT month_order_activated_local_at
    AS month
   , a.order_country_code AS countrynn
   , COUNT (DISTINCT CASE WHEN DATE_TRUNC('month'
   , d.registration_date_local) = DATE_TRUNC('month'
   , a.order_activated_local_at) THEN a.customer_id ELSE NULL END) AS new_user_and_new_customers
   , COUNT (DISTINCT CASE WHEN DATE_TRUNC('month'
   , d.registration_date_local)
   < DATE_TRUNC('month'
   , a.order_activated_local_at) THEN a.customer_id ELSE NULL END) AS old_user_and_new_customers
FROM info_period a
    LEFT JOIN info_users_period d
ON a.customer_id = d.user_id
WHERE a.order_is_first_delivered_order = true
GROUP BY 1, 2
    )
        , customers_and_orders AS (
WITH orders_per_period AS (
    WITH orders_per_customer AS (
    SELECT DISTINCT month_order_activated_local_at AS date
        , order_country_code AS countryname
        , customer_id
        , COUNT (order_is_prime) AS prime_orders
        , COUNT (order_id) AS number_of_orders
        , COUNT (
    CASE
    WHEN mcd_cities = true then order_id
    else null
    end
    ) as number_of_orders_mcd
        , COUNT (
    CASE
    WHEN mcd_cities = true and rank_month = 1 then order_id
    else null
    end
    ) as number_of_orders_mcd_1st
        , MAX (order_is_first_delivered_order) AS is_new_customer
    FROM info_period_rank
    GROUP BY 1, 2, 3)
    SELECT DISTINCT a.date
        , a.countryname
        , a.customer_id
        , a.prime_orders
        , a.number_of_orders
        , a.number_of_orders_mcd
        , a.number_of_orders_mcd_1st
        , is_new_customer
        , CASE
    WHEN a.date = date_trunc('month', subscription_period_started_at) THEN 'Prime_User'
    WHEN a.date = date_trunc('month', subscription_period_expired_at) THEN 'Prime_User'
    ELSE 'Non_Prime' END sub_type
    FROM orders_per_customer a
    LEFT JOIN delta.growth_prime_odp.prime_subscriptions_v2 b
    on (a.customer_id = b.customer_id and a.countryname = b.country_code and (a.date = date_trunc('month', subscription_period_started_at) OR a.date = date_trunc('month', subscription_period_expired_at)))
    )

SELECT DISTINCT
    COALESCE (this_month.countryname, last_month.countryname) AS countryor
        , COALESCE (this_month.date, DATE_ADD('month', 1, last_month.date)) AS date
        , SUM (CASE WHEN this_month.is_new_customer = true THEN 1 ELSE 0 END) AS nc
    --, SUM (CASE WHEN this_month.is_new_customer = true AND this_month.number_of_orders_mcd > 0 THEN 1 ELSE 0 END) AS nc_mcd,
        , SUM (CASE WHEN this_month.is_new_customer = true AND this_month.number_of_orders_mcd_1st > 0 THEN 1 ELSE 0 END) AS nc_mcd_1st
        , SUM (CASE WHEN this_month.is_new_customer = false AND last_month.is_new_customer = true THEN 1 ELSE 0 END) AS recurrent_nc
        , SUM (CASE WHEN this_month.is_new_customer = false AND last_month.is_new_customer = true AND this_month.number_of_orders_mcd > 0 THEN 1 ELSE 0 END) AS recurrent_nc_mcd
        , SUM (CASE WHEN this_month.is_new_customer = false AND last_month.is_new_customer = false THEN 1 ELSE 0 END) AS recurrent_rc
        , SUM (CASE WHEN this_month.is_new_customer = false AND last_month.is_new_customer = false AND this_month.number_of_orders_mcd > 0 THEN 1 ELSE 0 END) AS recurrent_rc_mcd
        , SUM (CASE WHEN this_month.date IS NULL AND last_month.date IS NOT NULL AND last_month.is_new_customer = true THEN 1 ELSE 0 END) AS churned_nc
        , SUM (CASE WHEN this_month.date IS NULL AND last_month.date IS NOT NULL AND last_month.is_new_customer = false THEN 1 ELSE 0 END) AS churned_rc
        , SUM (CASE WHEN this_month.is_new_customer = false AND last_month.date IS NULL THEN 1 ELSE 0 END) AS reactivated
    -- , SUM (CASE WHEN this_month.is_new_customer = false AND this_month.number_of_orders_mcd > 0 AND last_month.date IS NULL THEN 1 ELSE 0 END) AS reactivated_mcd
        , SUM (CASE WHEN this_month.is_new_customer = false AND this_month.number_of_orders_mcd_1st > 0 AND last_month.date IS NULL THEN 1 ELSE 0 END) AS reactivated_mcd_1st
        , SUM (CASE WHEN this_month.is_new_customer = true THEN this_month.number_of_orders ELSE 0 END) AS orders_nc
        , SUM (CASE WHEN this_month.is_new_customer = false AND last_month.is_new_customer = true THEN this_month.number_of_orders ELSE 0 END) AS orders_recurrent_nc
        , SUM (CASE WHEN this_month.is_new_customer = false AND last_month.is_new_customer = false THEN this_month.number_of_orders ELSE 0 END) AS orders_recurrent_rc
        , SUM (CASE WHEN this_month.is_new_customer = false AND last_month.date IS NULL THEN this_month.number_of_orders ELSE 0 END) AS orders_reactivated
FROM orders_per_period this_month
    FULL OUTER JOIN orders_per_period last_month
ON this_month.customer_id = last_month.customer_id
    AND this_month.countryname = last_month.countryname
    AND last_month.date = DATE_ADD('month', -1, this_month.date)

WHERE 1 = 1

GROUP BY 1, 2
    )
        , Country_Table AS (
SELECT
    date (day_date) AS Month
        , country_code
        , SUM (downloads_ios) AS downloads_ios
        , SUM (downloads_android) AS downloads_android
        , SUM (total_downloads) AS total_downloads
        , SUM (active_users_ios) AS active_users_ios
        , SUM (active_users_android) AS active_users_android
        , SUM (total_active_users) AS total_active_users

FROM "delta"."growth__app_annie_kpis__odp".kpi_app_usage_metrics_monthly

WHERE category = 'competitors' and p_execution_date = date_add('day', -1, current_date)
GROUP BY 1, 2)
        , App_Table AS (
SELECT
    app_name
        , date (day_date) AS Month
        , country_code
        , SUM (downloads_ios) AS downloads_ios
        , SUM (downloads_android) AS downloads_android
        , SUM (total_downloads) AS total_downloads
        , SUM (active_users_ios) AS active_users_ios
        , SUM (active_users_android) AS active_users_android
        , SUM (total_active_users) AS total_active_users

FROM "delta"."growth__app_annie_kpis__odp".kpi_app_usage_metrics_monthly

WHERE category = 'competitors' and p_execution_date = date_add('day', -1, current_date)

GROUP BY 1, 2, 3)
        , Last_Date AS (
SELECT
    country_code
        , MAX (date (day_date)) AS Last_Day_Available

FROM "delta"."growth__app_annie_kpis__odp".kpi_app_usage_metrics_daily

WHERE category = 'competitors' and p_execution_date = date_add('day', -1, current_date)

GROUP BY 1)
        , App_Annie_Downloads AS (
SELECT
    App_Table.Month AS Month
        , App_Table.country_code AS Country
        , App_Table.total_downloads AS AppAnnie_Glovo_Downloads
        , App_Table.total_active_users AS AppAnnnie_Glovo_Users
        , Country_Table.total_downloads AS AppAnnie_Total_Country_Category_Downloads
        , Country_Table.total_active_users AS AppAnnie_Total_Country_Category_Users_Total
        , Last_Day_Available AS AppAnnie_Last_Day_Available
        , TO_CHAR(current_timestamp, 'yyyy-mm-dd hh24:mi') AS update_time

FROM App_Table
    LEFT JOIN Country_Table
ON App_Table.country_code = Country_Table.country_code AND App_Table.Month = Country_Table.Month
    LEFT JOIN Last_Date ON Country_Table.country_code = Last_Date.country_code

WHERE App_Table.app_name = 'Glovo')


    , trampa AS (
SELECT *
from number_signups
    FULL JOIN active_customers
ON users_registration_month = monthac AND countryns = countryac
    FULL JOIN newnew_newrecurrent ON users_registration_month = month AND countryns = countrynn
    FULL JOIN customers_and_orders ON users_registration_month = date AND countryns = countryor
    FULL JOIN number_downloads ON users_registration_month = download_time AND countryns = countryd
ORDER BY 1, 2
    )

SELECT countryor AS country
     , users_registration_month
     , users_count
     , number_of_active_customers
     , nc
     , recurrent_nc
     , recurrent_rc
     , reactivated
     , reactivated_mcd_1st
     , nc_mcd_1st
     , recurrent_nc_mcd
     , recurrent_rc_mcd


FROM trampa a
         LEFT JOIN App_Annie_Downloads b ON a.users_registration_month = b.Month AND a.countryns = b.Country
WHERE users_registration_month IS NOT NULL
  AND number_of_active_customers IS NOT NULL
  AND monthac IS NOT NULL
  AND countryns IN ('PL')

ORDER BY 1, 2