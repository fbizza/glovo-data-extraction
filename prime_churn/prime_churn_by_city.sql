SELECT ps.country_code,
       ps.preferred_city_code
        , date (ps.subscription_period_expired_at) as period_end_date
        , date (date_trunc('month', ps.subscription_period_expired_at)) AS month
        , case when ps.unsubscribed_at is not null and (ps.next_csp_started_at is null or ps_next.subscription_rank = ps.subscription_rank + 1) then 'unsubscribed' else 'didnt unsubscribe'
end
as renewed_or_not
     , count(distinct ps.customer_id) as subscribers
FROM delta.growth_prime_odp.prime_subscriptions_v2 as ps

    LEFT JOIN delta.growth_prime_odp.prime_subscriptions_v2 AS ps_next
        ON ps_next.customer_id = ps.customer_id
        AND ps_next.customer_period_rank = ps.customer_period_rank + 1

WHERE ps.subscription_period_expired_at >= current_timestamp - interval '2' month
AND ps.subscription_period_expired_at <= current_date
AND ps.country_code = 'PL'
group by 1, 2, 3, 4, 5
order by 3