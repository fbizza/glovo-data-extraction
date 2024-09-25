WITH installs AS (
    SELECT
        date_trunc('month', p_event_date) AS month,
        count(*) AS installs
    FROM
        delta.growth_adjust_odp.adjust_install_v3
    WHERE
        app_install_country = 'PL'
        AND app_install_event_time >= date('2022-01-01')
    GROUP BY
        1
),
signups AS (
    SELECT
        date_trunc('month', user_created_at) AS month,
        count(DISTINCT user_id) AS sign_ups
    FROM
        delta.central_users_odp.users_v2 a
    LEFT JOIN
        delta.customer_attributes_odp.customers_v1 b
        ON a.user_id = b.id
    JOIN
        delta.central_geography_odp.cities_v2 c
        ON c.city_code = b.preferred_city_code
    WHERE
        user_created_at >= DATE('2022-01-01')
        AND c.country_code = 'PL'
    GROUP BY
        1
),
new_customers AS (
    SELECT
        date_trunc('month', order_started_local_at) AS month,
        count(distinct o.customer_id) AS new_customers
    FROM
        delta.central_order_descriptors_odp.order_descriptors_v2 o
    WHERE
        o.order_country_code = 'PL'
        AND o.order_final_status = 'DeliveredStatus'
        AND o.order_parent_relationship_type IS NULL
        AND date(order_started_local_at) >= date '2022-01-01'
        AND o.order_is_first_delivered_order
    GROUP BY
        1
)
SELECT
    date(COALESCE(i.month, s.month, n.month)) AS month,
    COALESCE(i.installs, 0) AS installs,
    COALESCE(s.sign_ups, 0) AS sign_ups,
    COALESCE(n.new_customers, 0) AS new_customers,
    1.0000*COALESCE(n.new_customers, 0) / COALESCE(i.installs, 0) as nc_over_installs
FROM
    installs i
JOIN
    signups s
    ON i.month = s.month
JOIN
    new_customers n
    ON COALESCE(i.month, s.month) = n.month
ORDER BY
    month ASC;
