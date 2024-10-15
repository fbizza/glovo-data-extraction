
--usa questa:
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
    a.promocode_id IN (2615170513, 2597604350)
GROUP BY
a.promocode_id

-- order_ids associati ad un certo promocode:
SELECT
count(order_id)
FROM
    "delta"."growth_discounts_odp".discounts_promocode_uses a
FULL OUTER JOIN
    delta.growth_pricing_discounts_odp.pricing_discounts b
    ON a.promocode_id = b.promocode_id
    AND a.promocode_uses_id = b.promocode_use_id
WHERE
    a.promocode_id IN (2615170513, 2597604350)

