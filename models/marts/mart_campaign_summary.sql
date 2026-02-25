{{ config(materialized='table') }}

with fct_ads as (
    select * from {{ ref('fct_ads_performance') }}
),

dim_lookup as (
    -- keep only keys to avoid accidental duplicate joins
    select distinct
        ad_id,
        campaign_id
    from {{ ref('dim_ads') }}
)

select
    f.*,
    case
        when d.ad_id is null then 'unknown'
        else 'known'
    end as dim_info_available
from fct_ads f
left join dim_lookup d
    on f.ad_id = d.ad_id
   and f.campaign_id = d.campaign_id