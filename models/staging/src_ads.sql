{{ config(materialized='view') }}

with raw_ads as (
    select * from {{ source('ads_raw', 'r_ads') }}
)

select
    ad_id,
    reporting_start,
    reporting_end,
    campaign_id,
    fb_campaign_id,
    age,
    gender,
    interest1,
    interest2,
    interest3,
    impressions,
    clicks,
    spent,
    total_conversion,
    approved_conversion
from raw_ads