SELECT
   count(promocode_uses_id) as insertions
FROM "delta".growth_discounts_odp.discounts_promocode_uses pu
         LEFT JOIN delta.growth_discounts_odp.discounts_promocodes pc
                   ON pc.promocode_id = pu.promocode_id
WHERE pc.promotion_id in (32042682,31184592)
