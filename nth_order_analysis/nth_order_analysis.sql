WITH numbered_orders AS (
    SELECT
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_started_local_at) AS order_number,
        o.*
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
order_counts AS (
    SELECT
        no.order_number,
        sf.filter,
        COUNT(*) AS order_count
    FROM
        numbered_orders no
    JOIN
        store_filters sf ON no.store_id = sf.store_id
    WHERE no.order_number <= 15
    GROUP BY
        no.order_number,
        sf.filter
),
total_orders AS (
    SELECT
        order_number,
        SUM(order_count) AS total_count
    FROM
        order_counts
    GROUP BY
        order_number
),
percentages AS (
    SELECT
        oc.order_number,
        oc.filter,
        (1.0000*oc.order_count / to.total_count) * 100 AS percentage
    FROM
        order_counts oc
    JOIN
        total_orders to ON oc.order_number = to.order_number
)
SELECT
    filter,
    MAX(CASE WHEN order_number = 1 THEN percentage ELSE 0 END) AS "1st_order",
    MAX(CASE WHEN order_number = 2 THEN percentage ELSE 0 END) AS "2nd_order",
    MAX(CASE WHEN order_number = 3 THEN percentage ELSE 0 END) AS "3rd_order",
    MAX(CASE WHEN order_number = 4 THEN percentage ELSE 0 END) AS "4th_order",
    MAX(CASE WHEN order_number = 5 THEN percentage ELSE 0 END) AS "5th_order",
    MAX(CASE WHEN order_number = 6 THEN percentage ELSE 0 END) AS "6th_order",
    MAX(CASE WHEN order_number = 7 THEN percentage ELSE 0 END) AS "7th_order",
    MAX(CASE WHEN order_number = 8 THEN percentage ELSE 0 END) AS "8th_order",
    MAX(CASE WHEN order_number = 9 THEN percentage ELSE 0 END) AS "9th_order",
    MAX(CASE WHEN order_number = 10 THEN percentage ELSE 0 END) AS "10th_order",
    MAX(CASE WHEN order_number = 11 THEN percentage ELSE 0 END) AS "11th_order",
    MAX(CASE WHEN order_number = 12 THEN percentage ELSE 0 END) AS "12th_order",
    MAX(CASE WHEN order_number = 13 THEN percentage ELSE 0 END) AS "13th_order",
    MAX(CASE WHEN order_number = 14 THEN percentage ELSE 0 END) AS "14th_order",
    MAX(CASE WHEN order_number = 15 THEN percentage ELSE 0 END) AS "15th_order"
FROM
    percentages
GROUP BY
    filter
ORDER BY
    filter;