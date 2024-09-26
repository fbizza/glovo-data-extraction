SELECT
    date(date_trunc('day', partner_ops_kpis.p_day_date)) as date,
    COALESCE(SUM(cancellation_dtp.cancelled_orders_cdtp ), 0) AS cancelled_orders_dtp
FROM delta.partner__details__odp.partner_infos_said_day_lv_order_based  AS partner_ops_kpis
LEFT JOIN delta.partner__cancellation_dtp__odp.canc_dtp_said_day_lv  AS cancellation_dtp ON partner_ops_kpis.pk
      = cancellation_dtp.pk
WHERE partner_ops_kpis.order_country_code = 'PL' AND partner_ops_kpis.store_name = 'McDonald''s'
and partner_ops_kpis.p_day_date > date '2024-09-20'
GROUP BY 1
order by 1 asc