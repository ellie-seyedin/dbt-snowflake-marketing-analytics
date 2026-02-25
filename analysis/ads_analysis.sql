with campaign_summary as (
    select
        campaign_id,
        round(avg(ctr), 6) as avg_ctr,
        sum(impressions) as total_impressions,
        sum(clicks) as total_clicks,
        round(sum(spent), 4) as total_spent,
        sum(total_conversion) as total_conversions,
        sum(approved_conversion) as approved_conversions
    from {{ ref('fct_ads_performance') }}
    group by campaign_id
    having sum(impressions) > 1000   -- filter tiny campaigns (adjust threshold)
),

campaign_dim as (
    select
        campaign_id,
        min(fb_campaign_id) as fb_campaign_id   -- simple representative value
    from {{ ref('dim_ads') }}
    group by campaign_id
)

select
    cs.campaign_id,
    cd.fb_campaign_id,
    cs.avg_ctr,
    cs.total_impressions,
    cs.total_clicks,
    cs.total_spent,
    cs.total_conversions,
    cs.approved_conversions
from campaign_summary cs
left join campaign_dim cd
    on cs.campaign_id = cd.campaign_id
order by cs.avg_ctr desc
limit 20