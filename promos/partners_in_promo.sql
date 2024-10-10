select date (date_trunc('day', order_started_local_at)) AS day,
       count(distinct case when discount_subtype = 'PROMOTION_BASKET_DISCOUNT' then store_name else null end) as basket_discount_partners,
       count(distinct case when discount_subtype = 'PROMOTION_FREE_DELIVERY' then store_name else null end) as free_delivery_partners,
       count(distinct case when discount_subtype = 'PROMOTION_FLAT_DELIVERY' then store_name else null end) as flat_delivery_partners,
       count(distinct case when discount_subtype = 'PROMOTION_PERCENTAGE_DISCOUNT' then store_name else null end) as percentage_discount_partners,
       count(distinct case when discount_subtype = 'PROMOTION_TWO_FOR_ONE' then store_name else null end) as two_for_one_partners
from (delta.central_order_descriptors_odp.order_descriptors_v2 o left join delta.growth_pricing_discounts_odp.pricing_discounts p
    on p.order_id = o.order_id)
where
    o.order_country_code = 'PL'
  AND o.order_final_status = 'DeliveredStatus'
  AND o.order_parent_relationship_type IS NULL
  AND o.order_started_local_at > date '2024-09-30'
group by 1
order by 1 desc
