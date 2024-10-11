WITH numbered_orders AS (
    SELECT
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_started_local_at) AS order_number,
        o.customer_id,
        o.store_id
    FROM
        delta.central_order_descriptors_odp.order_descriptors_v2 o
    WHERE o.order_country_code = 'PL'
        AND o.order_final_status = 'DeliveredStatus'
        AND o.order_parent_relationship_type IS NULL
),
store_filters AS (
    SELECT
        store_search_filters.store_id AS store_id,
        store_search_filters.filter AS filter
    FROM
        delta.partner_stores_odp.store_search_filters AS store_search_filters
),
initial_customers AS (
    SELECT
        no.customer_id,
        sf.filter,
        no.order_number
    FROM
        numbered_orders no
    JOIN
        store_filters sf ON no.store_id = sf.store_id
    WHERE no.order_number = 1
),
retention_counts AS (
    SELECT
        ic.filter,
        no.order_number,
        COUNT(DISTINCT no.customer_id) AS retained_customers
    FROM
        initial_customers ic
    JOIN
        numbered_orders no ON ic.customer_id = no.customer_id
    WHERE no.order_number <= 15
    GROUP BY
        ic.filter,
        no.order_number
),
initial_customer_counts AS (
    SELECT
        filter,
        COUNT(DISTINCT customer_id) AS initial_customers
    FROM
        initial_customers
    GROUP BY
        filter
)
SELECT
    rc.filter,
    MAX(CASE WHEN rc.order_number = 1 THEN (rc.retained_customers * 100.0 / icc.initial_customers) ELSE 0 END) AS "1st_order",
    MAX(CASE WHEN rc.order_number = 2 THEN (rc.retained_customers * 100.0 / icc.initial_customers) ELSE 0 END) AS "2nd_order",
    MAX(CASE WHEN rc.order_number = 3 THEN (rc.retained_customers * 100.0 / icc.initial_customers) ELSE 0 END) AS "3rd_order",
    MAX(CASE WHEN rc.order_number = 4 THEN (rc.retained_customers * 100.0 / icc.initial_customers) ELSE 0 END) AS "4th_order",
    MAX(CASE WHEN rc.order_number = 5 THEN (rc.retained_customers * 100.0 / icc.initial_customers) ELSE 0 END) AS "5th_order",
    MAX(CASE WHEN rc.order_number = 6 THEN (rc.retained_customers * 100.0 / icc.initial_customers) ELSE 0 END) AS "6th_order",
    MAX(CASE WHEN rc.order_number = 7 THEN (rc.retained_customers * 100.0 / icc.initial_customers) ELSE 0 END) AS "7th_order",
    MAX(CASE WHEN rc.order_number = 8 THEN (rc.retained_customers * 100.0 / icc.initial_customers) ELSE 0 END) AS "8th_order",
    MAX(CASE WHEN rc.order_number = 9 THEN (rc.retained_customers * 100.0 / icc.initial_customers) ELSE 0 END) AS "9th_order",
    MAX(CASE WHEN rc.order_number = 10 THEN (rc.retained_customers * 100.0 / icc.initial_customers) ELSE 0 END) AS "10th_order",
    MAX(CASE WHEN rc.order_number = 11 THEN (rc.retained_customers * 100.0 / icc.initial_customers) ELSE 0 END) AS "11th_order",
    MAX(CASE WHEN rc.order_number = 12 THEN (rc.retained_customers * 100.0 / icc.initial_customers) ELSE 0 END) AS "12th_order",
    MAX(CASE WHEN rc.order_number = 13 THEN (rc.retained_customers * 100.0 / icc.initial_customers) ELSE 0 END) AS "13th_order",
    MAX(CASE WHEN rc.order_number = 14 THEN (rc.retained_customers * 100.0 / icc.initial_customers) ELSE 0 END) AS "14th_order",
    MAX(CASE WHEN rc.order_number = 15 THEN (rc.retained_customers * 100.0 / icc.initial_customers) ELSE 0 END) AS "15th_order"
FROM
    retention_counts rc
JOIN
    initial_customer_counts icc ON rc.filter = icc.filter
GROUP BY
    rc.filter
ORDER BY
    rc.filter