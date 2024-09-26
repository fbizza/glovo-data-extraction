WITH mcdonalds_orders_last_year AS (
  SELECT
    date_trunc('day', order_started_local_at) AS day,
    COUNT(order_id) AS mcdonalds_orders
  FROM
    delta.central_order_descriptors_odp.order_descriptors_v2
  WHERE
    order_country_code = 'PL'
    AND order_parent_relationship_type IS NULL
    AND order_started_local_at >= DATE('2023-11-20')
    AND order_final_status = 'DeliveredStatus'
    AND store_name = 'McDonald''s'
    AND p_creation_date > DATE('2023-08-01')
  GROUP BY 1
),
mcdonalds_orders_this_year AS (
  SELECT
    date_trunc('day', order_started_local_at) AS day,
    COUNT(order_id) AS mcdonalds_orders
  FROM
    delta.central_order_descriptors_odp.order_descriptors_v2
  WHERE
    order_country_code = 'PL'
    AND order_parent_relationship_type IS NULL
    AND order_started_local_at >= DATE('2024-09-20')
    AND order_final_status = 'DeliveredStatus'
    AND store_name = 'McDonald''s'
    AND p_creation_date > DATE('2023-08-01')
  GROUP BY 1
),
total_orders_last_year AS (
  SELECT
    date_trunc('day', order_started_local_at) AS day,
    COUNT(order_id) AS total_orders
  FROM
    delta.central_order_descriptors_odp.order_descriptors_v2
  WHERE
    order_country_code = 'PL'
    AND order_parent_relationship_type IS NULL
    AND order_started_local_at >= DATE('2023-11-20')
    AND order_final_status = 'DeliveredStatus'
    AND p_creation_date > DATE('2023-08-01')
  GROUP BY 1
),
total_orders_this_year AS (
  SELECT
    date_trunc('day', order_started_local_at) AS day,
    COUNT(order_id) AS total_orders
  FROM
    delta.central_order_descriptors_odp.order_descriptors_v2
  WHERE
    order_country_code = 'PL'
    AND order_parent_relationship_type IS NULL
    AND order_started_local_at >= DATE('2024-09-20')
    AND order_final_status = 'DeliveredStatus'
    AND p_creation_date > DATE('2023-08-01')
  GROUP BY 1
),
product_orders_last_year AS (
  SELECT
    date_trunc('day', o.order_started_local_at) AS day,
    COUNT(DISTINCT o.order_id) AS product_orders
  FROM
    delta.central_order_descriptors_odp.order_descriptors_v2 o
  LEFT JOIN
    delta.customer_bought_products_odp.bought_products_v2 b ON b.order_id = o.order_id
  WHERE
    o.order_country_code = 'PL'
    AND o.order_final_status = 'DeliveredStatus'
    AND o.order_parent_relationship_type IS NULL
    AND o.store_name = 'McDonald''s'
    AND UPPER(b.product_name) LIKE UPPER('%drwal%')
    AND order_started_local_at >= DATE('2023-11-20')
    AND o.p_creation_date > DATE('2023-08-01') AND b.p_creation_date > DATE('2023-08-01')
  GROUP BY 1
),
product_orders_this_year AS (
  SELECT
    date_trunc('day', o.order_started_local_at) AS day,
    COUNT(DISTINCT o.order_id) AS product_orders
  FROM
    delta.central_order_descriptors_odp.order_descriptors_v2 o
  LEFT JOIN
    delta.customer_bought_products_odp.bought_products_v2 b ON b.order_id = o.order_id
  WHERE
    o.order_country_code = 'PL'
    AND o.order_final_status = 'DeliveredStatus'
    AND o.order_parent_relationship_type IS NULL
    AND o.store_name = 'McDonald''s'
    AND UPPER(b.product_name) LIKE UPPER('%drwal%')
    AND order_started_local_at >= DATE('2024-09-20')
    AND o.p_creation_date > DATE('2023-08-01') AND b.p_creation_date > DATE('2023-08-01')
  GROUP BY 1
),
cancelled_orders_last_year AS (
  SELECT
    date_trunc('day', partner_ops_kpis.p_day_date) AS day,
    COALESCE(SUM(cancellation_dtp.cancelled_orders_cdtp), 0) AS cancelled_orders
  FROM
    delta.partner__details__odp.partner_infos_said_day_lv_order_based AS partner_ops_kpis
  LEFT JOIN
    delta.partner__cancellation_dtp__odp.canc_dtp_said_day_lv AS cancellation_dtp ON partner_ops_kpis.pk = cancellation_dtp.pk
  WHERE
    partner_ops_kpis.order_country_code = 'PL'
    AND partner_ops_kpis.store_name = 'McDonald''s'
    AND partner_ops_kpis.p_day_date >= DATE('2023-11-20')
  GROUP BY 1
),
cancelled_orders_this_year AS (
  SELECT
    date_trunc('day', partner_ops_kpis.p_day_date) AS day,
    COALESCE(SUM(cancellation_dtp.cancelled_orders_cdtp), 0) AS cancelled_orders
  FROM
    delta.partner__details__odp.partner_infos_said_day_lv_order_based AS partner_ops_kpis
  LEFT JOIN
    delta.partner__cancellation_dtp__odp.canc_dtp_said_day_lv AS cancellation_dtp ON partner_ops_kpis.pk = cancellation_dtp.pk
  WHERE
    partner_ops_kpis.order_country_code = 'PL'
    AND partner_ops_kpis.store_name = 'McDonald''s'
    AND partner_ops_kpis.p_day_date >= DATE('2024-09-20')
  GROUP BY 1
),
aligned_orders_last_year AS (
  SELECT
    day AS actual_date_last_year,
    mcdonalds_orders,
    ROW_NUMBER() OVER (ORDER BY day) AS day_number
  FROM
    mcdonalds_orders_last_year
),
aligned_orders_this_year AS (
  SELECT
    day AS actual_date_this_year,
    mcdonalds_orders,
    ROW_NUMBER() OVER (ORDER BY day) AS day_number
  FROM
    mcdonalds_orders_this_year
),
aligned_total_orders_last_year AS (
  SELECT
    day AS actual_date_last_year,
    total_orders,
    ROW_NUMBER() OVER (ORDER BY day) AS day_number
  FROM
    total_orders_last_year
),
aligned_total_orders_this_year AS (
  SELECT
    day AS actual_date_this_year,
    total_orders,
    ROW_NUMBER() OVER (ORDER BY day) AS day_number
  FROM
    total_orders_this_year
),
aligned_product_orders_last_year AS (
  SELECT
    day AS actual_date_last_year,
    product_orders,
    ROW_NUMBER() OVER (ORDER BY day) AS day_number
  FROM
    product_orders_last_year
),
aligned_product_orders_this_year AS (
  SELECT
    day AS actual_date_this_year,
    product_orders,
    ROW_NUMBER() OVER (ORDER BY day) AS day_number
  FROM
    product_orders_this_year
),
aligned_cancelled_orders_last_year AS (
  SELECT
    day AS actual_date_last_year,
    cancelled_orders,
    ROW_NUMBER() OVER (ORDER BY day) AS day_number
  FROM
    cancelled_orders_last_year
),
aligned_cancelled_orders_this_year AS (
  SELECT
    day AS actual_date_this_year,
    cancelled_orders,
    ROW_NUMBER() OVER (ORDER BY day) AS day_number
  FROM
    cancelled_orders_this_year
),
comparison AS (
  SELECT
    this_year.day_number AS promotion_day,
    this_year.actual_date_this_year,
    last_year.actual_date_last_year,
    this_year.mcdonalds_orders AS mcdonalds_orders_this_year,
    last_year.mcdonalds_orders AS mcdonalds_orders_last_year,
    total_this_year.total_orders AS total_orders_this_year,
    total_last_year.total_orders AS total_orders_last_year,
    product_this_year.product_orders AS product_orders_this_year,
    product_last_year.product_orders AS product_orders_last_year,
    cancelled_this_year.cancelled_orders AS cancelled_orders_this_year,
    cancelled_last_year.cancelled_orders AS cancelled_orders_last_year,
    (this_year.mcdonalds_orders - last_year.mcdonalds_orders) AS absolute_difference,
    (1.000 * (this_year.mcdonalds_orders - last_year.mcdonalds_orders) / NULLIF(last_year.mcdonalds_orders, 0)) * 100 AS yoy_growth,
    (1.000 * this_year.mcdonalds_orders / NULLIF(total_this_year.total_orders, 0)) * 100 AS share_of_mcdonalds_orders_this_year,
    (1.000 * last_year.mcdonalds_orders / NULLIF(total_last_year.total_orders, 0)) * 100 AS share_of_mcdonalds_orders_last_year
  FROM
    aligned_orders_this_year this_year
  LEFT JOIN
    aligned_orders_last_year last_year ON this_year.day_number = last_year.day_number
  LEFT JOIN
    aligned_total_orders_this_year total_this_year ON this_year.day_number = total_this_year.day_number
  LEFT JOIN
    aligned_total_orders_last_year total_last_year ON last_year.day_number = total_last_year.day_number
  LEFT JOIN
    aligned_product_orders_this_year product_this_year ON this_year.day_number = product_this_year.day_number
  LEFT JOIN
    aligned_product_orders_last_year product_last_year ON last_year.day_number = product_last_year.day_number
  LEFT JOIN
    aligned_cancelled_orders_this_year cancelled_this_year ON this_year.day_number = cancelled_this_year.day_number
  LEFT JOIN
    aligned_cancelled_orders_last_year cancelled_last_year ON last_year.day_number = cancelled_last_year.day_number
)
SELECT
  promotion_day,
  date(actual_date_this_year) AS actual_date_this_year,
  date(actual_date_last_year) AS actual_date_last_year,
  mcdonalds_orders_this_year,
  mcdonalds_orders_last_year,
  total_orders_this_year,
  total_orders_last_year,
  product_orders_this_year,
  product_orders_last_year,
  cancelled_orders_this_year,
  cancelled_orders_last_year,
  absolute_difference,
  yoy_growth,
  share_of_mcdonalds_orders_this_year,
  share_of_mcdonalds_orders_last_year
FROM
  comparison
ORDER BY
  promotion_day ASC;