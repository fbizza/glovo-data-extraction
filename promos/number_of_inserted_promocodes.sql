SELECT
   promotion_id, count(promocode_uses_id) as insertions
FROM "delta".growth_discounts_odp.discounts_promocode_uses pu
         LEFT JOIN delta.growth_discounts_odp.discounts_promocodes pc
                   ON pc.promocode_id = pu.promocode_id
WHERE pc.promotion_id in (28205777,32042682, 30602898)
GROUP BY 1