WITH installs AS (
    SELECT
        date_trunc('week', p_event_date) AS week,
        count(*) AS installs
    FROM
        delta.growth_adjust_odp.adjust_install_v3
    WHERE
        app_install_country = 'PL'
        AND app_install_event_time >= date('2024-08-05')
    GROUP BY
        1
),
signups AS (
    SELECT
        date_trunc('week', user_created_at) AS week,
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
        user_created_at >= DATE('2024-08-05')
        AND c.country_code = 'PL'
    GROUP BY
        1
),
new_customers AS (
    SELECT
        date_trunc('week', order_started_local_at) AS week,
        count(distinct o.customer_id) AS new_customers
    FROM
        delta.central_order_descriptors_odp.order_descriptors_v2 o
    WHERE
        o.order_country_code = 'PL'
        AND o.order_final_status = 'DeliveredStatus'
        AND o.order_parent_relationship_type IS NULL
        AND date(order_started_local_at) >= date '2024-08-05'
        AND o.order_is_first_delivered_order
    GROUP BY
        1
)
SELECT
    date(COALESCE(i.week, s.week, n.week)) AS week,
    COALESCE(i.installs, 0) AS installs,
    COALESCE(s.sign_ups, 0) AS sign_ups,
    COALESCE(n.new_customers, 0) AS new_customers
FROM
    installs i
JOIN
    signups s
    ON i.week = s.week
JOIN
    new_customers n
    ON COALESCE(i.week, s.week) = n.week
ORDER BY
    week ASC;

