SELECT o.order_id, sum(bought_product_quantity) as n_items
FROM delta.central_order_descriptors_odp.order_descriptors_v2 o left join delta.customer_bought_products_odp.bought_products_v2 p on o.order_id = p.order_id
WHERE o.order_country_code = 'PL'
    AND o.order_final_status = 'DeliveredStatus'
    AND o.order_parent_relationship_type IS NULL
    AND order_code = 'WABLTMUZB'
group by 1


