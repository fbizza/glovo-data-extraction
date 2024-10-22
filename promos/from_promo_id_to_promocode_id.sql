SELECT
    a.promocode_id, b.promocode_promotion_id
FROM
    delta.growth_discounts_odp.discounts_promocode_uses a
    LEFT JOIN
    delta.growth_pricing_discounts_odp.pricing_discounts b
ON a.promocode_id = b.promocode_id
    AND a.promocode_uses_id = b.promocode_use_id
WHERE
    promocode_promotion_id IN (26734142)