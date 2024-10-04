SELECT
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_started_local_at) AS order_number,
    o.*
FROM
    delta.central_order_descriptors_odp.order_descriptors_v2 o
WHERE o.order_country_code = 'PL'
    AND o.order_final_status = 'DeliveredStatus'
    AND o.order_parent_relationship_type IS NULL



SELECT
    store_search_filters.store_id  AS "store_search_filters.store_id",
    store_search_filters.filter  AS "store_search_filters.filter"
FROM "delta"."partner_stores_odp"."store_search_filters"  AS store_search_filters
WHERE (store_search_filters.store_id ) = 47055


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
)
SELECT
    no.order_number,
    sf.filter,
    COUNT(*) AS order_count
FROM
    numbered_orders no
LEFT JOIN
    store_filters sf
ON
    no.store_id = sf.store_id
WHERE no.order_number <= 15
GROUP BY
    no.order_number,
    sf.filter
ORDER BY
    no.order_number,
    sf.filter;