SELECT
    nc_retention_m1_per_store.store_name  AS store_name,
    (DATE_FORMAT(CAST(nc_retention_m1_per_store.p_month_ret_m1_date  AS TIMESTAMP),'%Y-%m')) AS month,
    ROUND((AVG(nc_retention_m1_per_store.ret_m1 )) / AVG(nc_retention_m1_per_store.nc_m0), 4) AS retention
FROM delta.growth__international_growth_nc_retention_per_store__odp.nc_retention_per_store_ddp  AS nc_retention_m1_per_store
WHERE nc_retention_m1_per_store.order_country_code = 'PL' AND nc_retention_m1_per_store.store_name IN
    (
    'KFC',
    'McDonald''s',
    'Biedronka Express',
    'Sklep Internetowy Biedronka',
    'Zahir Kebab',
    'Auchan',
    'Domino''s Pizza',
    'Pizza Hut',
    'Duży Ben',
    'Kebab King',
    'Apteczka Zdrowia'
        )
GROUP BY 1, 2
ORDER BY 2, 1 ASC