
with gmv as (
SELECT
    (DATE_FORMAT(DATE_TRUNC('DAY', DATE_ADD('day', (0 - MOD((DAY_OF_WEEK(promos_okr_tracker.p_creation_date ) % 7) - 1 + 7, 7)), promos_okr_tracker.p_creation_date )), '%Y-%m-%d')) AS week,
    promos_okr_tracker.store_name as store_name,
    1.0000 *  ( COALESCE(SUM(promos_okr_tracker.total_promotool_discounts_partner_funded ), 0) ) / nullif(( COALESCE(SUM(promos_okr_tracker.theoretical_gmv_partner ), 0) ), 0) AS gmv_discounted_partner,
    COALESCE(SUM(promos_okr_tracker.theoretical_gmv ), 0) AS theoretical_gmv,
    COALESCE(SUM(promos_okr_tracker.total_promotool_discounts_partner_funded ), 0) AS total_discounts_partner_funded,
    COALESCE(SUM(promos_okr_tracker.total_promotool_discounts ), 0) AS total_discounts,
    COALESCE(SUM(promos_okr_tracker.total_promotool_discounts_glovo_funded ), 0) AS total_discounts_glovo_funded
FROM delta.growth__promos_okr__odp.promos_okr_tracker_v2  AS promos_okr_tracker
WHERE ((( promos_okr_tracker.p_creation_date  ) >= ((DATE_ADD('year', -1, DATE_TRUNC('YEAR', CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP))))) AND ( promos_okr_tracker.p_creation_date  ) < ((DATE_ADD('year', 2, DATE_ADD('year', -1, DATE_TRUNC('YEAR', CAST(CAST(DATE_TRUNC('DAY', NOW()) AS DATE) AS TIMESTAMP))))))))
  AND (promos_okr_tracker.country_code ) = 'PL'
  AND (promos_okr_tracker.store_id ) IN (47055, 47057, 47061, 47100, 51062, 85381, 92500, 135276, 205021, 367676, 395747, 446268, 476137)
GROUP BY
    1, 2
ORDER BY
    1 DESC),

orders as (
SELECT
    date(date_trunc('week', order_started_local_at)) week,
    store_name,
    count(order_id) AS orders,
    count(case when order_is_first_delivered_order = true then customer_id else null end) AS new_customers,
    count(distinct customer_id) as num_active_users,
    ROUND(AVG(order_total_purchase_local*1.00),2) as avg_aov_pln
FROM delta.central_order_descriptors_odp.order_descriptors_v2 o
WHERE o.order_country_code = 'PL'
    AND o.order_city_code = 'WAW'
    AND o.order_final_status = 'DeliveredStatus'
    AND o.order_parent_relationship_type IS NULL
    AND date(date_trunc('week', order_started_local_at)) > date '2024-01-01'
    AND o.store_id in (367676, 47055, 47057, 395747, 47100, 51062, 205021, 47061, 135276, 92500, 476137, 446268, 85381)
    AND p_creation_date > date '2023-11-01'
   -- AND o.store_name = 'McDonald''s'
GROUP BY 1, 2
order by 1 desc
)

SELECT o.week,
       o.store_name,
       o.orders,
       o.new_customers,
       o.num_active_users,
       o.avg_aov_pln,
       total_discounts_glovo_funded / theoretical_gmv as gmv_discounted_glovo,
       total_discounts_partner_funded / theoretical_gmv as gmv_discounted_partner,
       round((total_discounts_glovo_funded + total_discounts_partner_funded) / theoretical_gmv, 3) as dGMV
FROM orders o left join gmv g on (date(o.week) = date(g.week) and o.store_name = g.store_name)




