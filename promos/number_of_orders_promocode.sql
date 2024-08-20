WITH orders AS (
    SELECT 
        promocode_promotion_id,
        COUNT(DISTINCT o.order_id) AS orders,
        COUNT(DISTINCT CASE WHEN order_total_purchase_local < 40 THEN o.order_id ELSE NULL END) AS less_than_40,
        COUNT(DISTINCT CASE WHEN order_total_purchase_local >= 40 AND order_total_purchase_local < 50 THEN o.order_id ELSE NULL END) AS in_40_50,
        COUNT(DISTINCT CASE WHEN order_total_purchase_local >= 50 AND order_total_purchase_local < 60 THEN o.order_id ELSE NULL END) AS in_50_60,
        COUNT(DISTINCT CASE WHEN order_total_purchase_local >= 60 THEN o.order_id ELSE NULL END) AS more_than_60
    FROM 
        delta.central_order_descriptors_odp.order_descriptors_v2 o
    LEFT JOIN 
        delta.growth_pricing_discounts_odp.pricing_discounts p
    ON 
        p.order_id = o.order_id
    WHERE 
        order_country_code = 'PL'
        AND o.order_final_status = 'DeliveredStatus'
        AND o.order_parent_relationship_type IS NULL
        AND promocode_promotion_id IN (32042682, 31184592, 28205777, 30602898)
    GROUP BY 
        promocode_promotion_id
),
insertions as (
    SELECT pc.promotion_id,
       COUNT(promocode_uses_id) AS insertions
    FROM delta.growth_discounts_odp.discounts_promocode_uses pu
             LEFT JOIN delta.growth_discounts_odp.discounts_promocodes pc
                       ON pc.promocode_id = pu.promocode_id
    WHERE pc.promotion_id IN (32042682, 31184592, 28205777, 30602898)
    GROUP BY 1
)
SELECT o.*, i.insertions FROM orders o left join insertions i on o.promocode_promotion_id = i.promotion_id;