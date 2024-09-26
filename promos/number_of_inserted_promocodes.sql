SELECT
   count(promocode_uses_id) as insertions
FROM "delta".growth_discounts_odp.discounts_promocode_uses pu
         LEFT JOIN delta.growth_discounts_odp.discounts_promocodes pc
                   ON pc.promocode_id = pu.promocode_id
WHERE pc.promotion_id in (32042682,31184592)

--usa questa:

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
    SELECT pc.promotion_id, pc.promocode_id,
       COUNT(promocode_uses_id) AS insertions --conta il numero di clienti distinti che lo hanno messo
    FROM delta.growth_discounts_odp.discounts_promocode_uses pu
             LEFT JOIN delta.growth_discounts_odp.discounts_promocodes pc
                       ON pc.promocode_id = pu.promocode_id
    WHERE pc.promotion_id IN (32042682, 31184592, 28205777, 30602898)
    GROUP BY 1, 2
)
SELECT o.*, i.* FROM orders o left join insertions i on o.promocode_promotion_id = i.promotion_id;


SELECT
a.promocode_id,
    COUNT(CASE WHEN a.promocode_uses_id IS NOT NULL THEN 1 END) AS times_inserted,
    COUNT(CASE WHEN b.promocode_use_id IS NOT NULL THEN 1 END) AS times_used, --orders
    SUM(b.order_total_purchase_eur) AS tot_basket
FROM
    "delta"."growth_discounts_odp".discounts_promocode_uses a
FULL OUTER JOIN
    delta.growth_pricing_discounts_odp.pricing_discounts b
    ON a.promocode_id = b.promocode_id
    AND a.promocode_uses_id = b.promocode_use_id
LEFT JOIN
    (SELECT DISTINCT customer_id FROM delta.central_order_descriptors_odp.order_descriptors_v2) o
    ON o.customer_id = a.customer_id
WHERE
    a.promocode_id IN (2580410773,2519344627, 2615170513, 2597604350)
    --and a.customer_id = 152487369
GROUP BY
a.promocode_id