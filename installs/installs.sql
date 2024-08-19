SELECT date (date_trunc('month', p_event_date)) AS d_month , count(*) as installs
FROM delta.growth_adjust_odp.adjust_install_v3
WHERE app_install_country = 'PL'
  AND app_install_event_time >= date ('2024-01-01')
GROUP BY 1
ORDER BY 1 ASC



