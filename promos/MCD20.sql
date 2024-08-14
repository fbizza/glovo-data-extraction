WITH nc_from_mcdonalds
AS (
	SELECT o.customer_id
	FROM delta.central_order_descriptors_odp.order_descriptors_v2 o
	WHERE o.order_country_code = 'PL'
		AND o.order_final_status = 'DeliveredStatus'
		AND o.order_parent_relationship_type IS NULL
		AND o.order_started_local_at >= (
			SELECT min(o2.order_started_local_at)
			FROM delta.central_order_descriptors_odp.order_descriptors_v2 o2
			LEFT JOIN delta.growth_pricing_discounts_odp.pricing_discounts p ON p.order_id = o2.order_id
			WHERE p.promocode_promotion_id = 29688897
				AND o2.order_final_status = 'DeliveredStatus'
				AND o2.order_parent_relationship_type IS NULL
			)
		AND o.order_started_local_at <= (
			SELECT max(o2.order_started_local_at)
			FROM delta.central_order_descriptors_odp.order_descriptors_v2 o2
			LEFT JOIN delta.growth_pricing_discounts_odp.pricing_discounts p ON p.order_id = o2.order_id
			WHERE p.promocode_promotion_id = 29688897
				AND o2.order_final_status = 'DeliveredStatus'
				AND o2.order_parent_relationship_type IS NULL
			)
		AND o.order_is_first_delivered_order
		AND o.store_name = 'McDonald''s'
	),
customers_who_used_promo
AS (
	SELECT customer_id
	FROM (
		delta.central_order_descriptors_odp.order_descriptors_v2 o LEFT JOIN delta.growth_pricing_discounts_odp.pricing_discounts p ON p.order_id = o.order_id
		)
	WHERE promocode_promotion_id = 29688897
		AND o.order_final_status = 'DeliveredStatus'
		AND o.order_parent_relationship_type IS NULL
	),
nc_who_used_promo
AS (
	SELECT customer_id
	FROM (
		delta.central_order_descriptors_odp.order_descriptors_v2 o LEFT JOIN delta.growth_pricing_discounts_odp.pricing_discounts p ON p.order_id = o.order_id
		)
	WHERE promocode_promotion_id = 29688897
		AND o.order_final_status = 'DeliveredStatus'
		AND o.order_parent_relationship_type IS NULL
	    AND order_is_first_delivered_order
	),
nc_from_mc_without_promo_code as (
SELECT customer_id
FROM nc_from_mcdonalds
WHERE customer_id NOT IN (SELECT customer_id FROM nc_who_used_promo)),
nc_from_any_other
AS (
	SELECT o.customer_id
	FROM delta.central_order_descriptors_odp.order_descriptors_v2 o
	WHERE o.order_country_code = 'PL'
		AND o.order_final_status = 'DeliveredStatus'
		AND o.order_parent_relationship_type IS NULL
		AND o.order_started_local_at >= (
			SELECT min(o2.order_started_local_at)
			FROM delta.central_order_descriptors_odp.order_descriptors_v2 o2
			LEFT JOIN delta.growth_pricing_discounts_odp.pricing_discounts p ON p.order_id = o2.order_id
			WHERE p.promocode_promotion_id = 29688897
				AND o2.order_final_status = 'DeliveredStatus'
				AND o2.order_parent_relationship_type IS NULL
			)
		AND o.order_started_local_at <= (
			SELECT max(o2.order_started_local_at)
			FROM delta.central_order_descriptors_odp.order_descriptors_v2 o2
			LEFT JOIN delta.growth_pricing_discounts_odp.pricing_discounts p ON p.order_id = o2.order_id
			WHERE p.promocode_promotion_id = 29688897
				AND o2.order_final_status = 'DeliveredStatus'
				AND o2.order_parent_relationship_type IS NULL
			)
		AND o.order_is_first_delivered_order
		AND o.store_name <> 'McDonald''s'
	),
    experimental_group as (
        select
        count(o.order_id) as num_orders,
        count(distinct customer_id) as num_active_users,
        ROUND(AVG(order_total_purchase_local*1.00),2) as aov,
        1.000*count(distinct o.order_id)/count(distinct customer_id) as freq,
        count(case when contribution_margin_eur < 0 then o.order_id else null end) negative_cm_orders,
        count(case when contribution_margin_eur > 0 then o.order_id else null end) positive_cm_orders,
        1.000*count(case when contribution_margin_eur < 0 then o.order_id else null end)/count(o.order_id) as negative_cm_orders_percentage,
        1.000*count(case when contribution_margin_eur > 0 then o.order_id else null end)/count(o.order_id) as positive_cm_orders_percentage,
        stddev(contribution_margin_eur) as stddev,
        avg (contribution_margin_eur) as avg_contribution_margin_eur
        from delta.central_order_descriptors_odp.order_descriptors_v2 o LEFT JOIN delta.finance_financial_reports_odp.pnl_order_level f
                ON o.order_id = f.order_id
        where order_final_status = 'DeliveredStatus'
        AND order_parent_relationship_type is null
        AND customer_id in (select * from nc_who_used_promo)
    ),
    control_group_1 as (
        select
        count(o.order_id) as num_orders,
        count(distinct customer_id) as num_active_users,
        ROUND(AVG(order_total_purchase_local*1.00),2) as aov,
        1.000*count(distinct o.order_id)/count(distinct customer_id) as freq,
        count(case when contribution_margin_eur < 0 then o.order_id else null end) negative_cm_orders,
        count(case when contribution_margin_eur > 0 then o.order_id else null end) positive_cm_orders,
        1.000*count(case when contribution_margin_eur < 0 then o.order_id else null end)/count(o.order_id) as negative_cm_orders_percentage,
        1.000*count(case when contribution_margin_eur > 0 then o.order_id else null end)/count(o.order_id) as positive_cm_orders_percentage,
        stddev(contribution_margin_eur) as stddev,
        avg (contribution_margin_eur) as avg_contribution_margin_eur
        from delta.central_order_descriptors_odp.order_descriptors_v2 o LEFT JOIN delta.finance_financial_reports_odp.pnl_order_level f
                ON o.order_id = f.order_id
        where order_final_status = 'DeliveredStatus'
        AND order_parent_relationship_type is null
        AND customer_id in (select * from nc_from_mc_without_promo_code)
    ),
    control_group_2 as (
        select
        count(o.order_id) as num_orders,
        count(distinct customer_id) as num_active_users,
        ROUND(AVG(order_total_purchase_local*1.00),2) as aov,
        1.000*count(distinct o.order_id)/count(distinct customer_id) as freq,
        count(case when contribution_margin_eur < 0 then o.order_id else null end) negative_cm_orders,
        count(case when contribution_margin_eur > 0 then o.order_id else null end) positive_cm_orders,
        1.000*count(case when contribution_margin_eur < 0 then o.order_id else null end)/count(o.order_id) as negative_cm_orders_percentage,
        1.000*count(case when contribution_margin_eur > 0 then o.order_id else null end)/count(o.order_id) as positive_cm_orders_percentage,
        stddev(contribution_margin_eur) as stddev,
        avg (contribution_margin_eur) as avg_contribution_margin_eur
        from delta.central_order_descriptors_odp.order_descriptors_v2 o LEFT JOIN delta.finance_financial_reports_odp.pnl_order_level f
                ON o.order_id = f.order_id
        where order_final_status = 'DeliveredStatus'
        AND order_parent_relationship_type is null
        AND customer_id in (select * from nc_from_any_other)
    )

select 'experimental', * from experimental_group
UNION
select 'control_1', * from control_group_1
UNION
select 'control_2', * from control_group_2
order by 1 asc

--Todo add retention rate