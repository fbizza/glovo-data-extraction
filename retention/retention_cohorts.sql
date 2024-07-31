----- NC RET - STORE GRANULARITY
with store_aux as (SELECT
    date_trunc('month',order_activated_local_at) as first_order_month,
    order_country_code,
    order_city_code,
    store_name,
    od.customer_id as nc_id
FROM "delta"."central_order_descriptors_odp"."order_descriptors_v2" od
WHERE
    order_is_first_delivered_order = true
    AND order_activated_local_at <= date_trunc('month',date_add('month',-1,current_date))
    AND year(order_activated_local_at) >= 2021
    AND order_final_status = 'DeliveredStatus'
    group by 1,2,3,4,5)

SELECT
    date(first_order_month) as p_month_ret_m1_date,
    sa.order_country_code,
    sa.order_city_code,
    sa.store_name,
    count(distinct sa.nc_id) as nc_m0,
    count(distinct (CASE WHEN date_diff('month', first_order_month, date_trunc('month',od.order_activated_local_at)) = 1 then sa.nc_id end)) as ret_m1
    from store_aux sa
left join "delta"."central_order_descriptors_odp"."order_descriptors_v2" od on sa.nc_id = od.customer_id
WHERE
    order_activated_local_at <= date_trunc('month', current_date) AND year (order_activated_local_at) >= 2021
    AND od.order_final_status = 'DeliveredStatus'
group by 1,2,3,4
order by 1 desc