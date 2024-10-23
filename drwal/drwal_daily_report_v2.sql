with variables as (
    select 40 as mbs_threshold
),
promocode_orders as (
SELECT
    order_id
FROM
    delta.growth_discounts_odp.discounts_promocode_uses a
    LEFT JOIN
    delta.growth_pricing_discounts_odp.pricing_discounts b
ON a.promocode_id = b.promocode_id
    AND a.promocode_uses_id = b.promocode_use_id
WHERE
    a.promocode_id IN (2615170513, 2597604350)
),
NC as (
  SELECT
    date(date_trunc('month', order_started_local_at)) AS month,
    COUNT(customer_id) as NCs
  FROM
    delta.central_order_descriptors_odp.order_descriptors_v2
  WHERE
    order_country_code = 'PL'
    AND order_parent_relationship_type IS NULL
    AND order_started_local_at >= date '2023-01-01'
    AND order_final_status = 'DeliveredStatus'
    AND order_is_first_delivered_order
--     AND store_name = 'McDonald''s'
--     AND p_creation_date > (SELECT creation_date_cutoff FROM vars)
  GROUP BY 1
  ORDER BY 1 DESC
),
mc_donalds_orders as (
  SELECT
    date(date_trunc('month', order_started_local_at)) AS month,
    COUNT(order_id) as mc_donalds_orders
  FROM
    delta.central_order_descriptors_odp.order_descriptors_v2
  WHERE
    order_country_code = 'PL'
    AND order_parent_relationship_type IS NULL
    AND order_started_local_at >= date '2023-01-01'
    AND order_final_status = 'DeliveredStatus'
    AND store_name = 'McDonald''s'
  GROUP BY 1
  ORDER BY 1 DESC
),
mc_donalds_orders_with_promocode as (
  SELECT
    date(date_trunc('month', order_started_local_at)) AS month,
    COUNT(order_id) as mc_donalds_orders_with_promocode
  FROM
    delta.central_order_descriptors_odp.order_descriptors_v2
  WHERE
    order_country_code = 'PL'
    AND order_parent_relationship_type IS NULL
    AND order_started_local_at >= date '2023-01-01'
    AND order_final_status = 'DeliveredStatus'
    AND store_name = 'McDonald''s'
    AND order_id in (SELECT * FROM promocode_orders)
  GROUP BY 1
  ORDER BY 1 DESC
),
total_orders as (
  SELECT
    date(date_trunc('month', order_started_local_at)) AS month,
    COUNT(order_id) as total_orders
  FROM
    delta.central_order_descriptors_odp.order_descriptors_v2
  WHERE
    order_country_code = 'PL'
    AND order_parent_relationship_type IS NULL
    AND order_started_local_at >= date '2023-01-01'
    AND order_final_status = 'DeliveredStatus'
  GROUP BY 1
  ORDER BY 1 DESC
),
mbs_eligibile as (
  SELECT
    date(date_trunc('month', order_started_local_at)) AS month,
    COUNT(order_id) as mbs_eligibile_total_orders
  FROM
    delta.central_order_descriptors_odp.order_descriptors_v2
  WHERE
    order_country_code = 'PL'
    AND order_parent_relationship_type IS NULL
    AND order_started_local_at >= date '2023-01-01'
    AND order_final_status = 'DeliveredStatus'
    AND order_total_purchase_local > (select mbs_threshold from variables)
  GROUP BY 1
  ORDER BY 1 DESC
),
mbs_eligibile_mcdo as (
  SELECT
    date(date_trunc('month', order_started_local_at)) AS month,
    COUNT(order_id) as mbs_eligibile_mcdo
  FROM
    delta.central_order_descriptors_odp.order_descriptors_v2
  WHERE
    order_country_code = 'PL'
    AND order_parent_relationship_type IS NULL
    AND order_started_local_at >= date '2023-01-01'
    AND order_final_status = 'DeliveredStatus'
    AND order_total_purchase_local > (select mbs_threshold from variables)
    AND store_name = 'McDonald''s'
  GROUP BY 1
  ORDER BY 1 DESC
),
mbs_eligibile_mcdo_with_promo as (
  SELECT
    date(date_trunc('month', order_started_local_at)) AS month,
    COUNT(order_id) as mbs_eligibile_mcdo_with_promo
  FROM
    delta.central_order_descriptors_odp.order_descriptors_v2
  WHERE
    order_country_code = 'PL'
    AND order_parent_relationship_type IS NULL
    AND order_started_local_at >= date '2023-01-01'
    AND order_final_status = 'DeliveredStatus'
    AND order_total_purchase_local > (select mbs_threshold from variables)
    AND store_name = 'McDonald''s'
    AND order_id in (SELECT * FROM promocode_orders)
  GROUP BY 1
  ORDER BY 1 DESC
)
SELECT
    nc.month,
    nc.NCs,
    mco.mc_donalds_orders,
    mcop.mc_donalds_orders_with_promocode,
    to.total_orders,
    me.mbs_eligibile_total_orders,
    mem.mbs_eligibile_mcdo,
    memp.mbs_eligibile_mcdo_with_promo
FROM
    NC nc
LEFT JOIN
    mc_donalds_orders mco ON nc.month = mco.month
LEFT JOIN
    mc_donalds_orders_with_promocode mcop ON nc.month = mcop.month
LEFT JOIN
    total_orders to ON nc.month = to.month
LEFT JOIN
    mbs_eligibile me ON nc.month = me.month
LEFT JOIN
    mbs_eligibile_mcdo mem ON nc.month = mem.month
LEFT JOIN
    mbs_eligibile_mcdo_with_promo memp ON nc.month = memp.month
ORDER BY
    nc.month DESC
