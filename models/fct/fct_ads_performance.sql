{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['ad_id', 'reporting_start', 'reporting_end', 'campaign_id', 'age', 'gender', 'interest1', 'interest2', 'interest3']
) }}

with src_ads as (
    select * from {{ ref('src_ads') }}
)

select
    ad_id,
    campaign_id,
    reporting_start,
    reporting_end,
    age,
    gender,
    interest1,
    interest2,
    interest3,
    impressions,
    clicks,
    round(spent, 4) as spent,
    total_conversion,
    approved_conversion,

    case when impressions > 0 then round(clicks / impressions, 6) end as ctr,
    case when clicks > 0 then round(spent / clicks, 4) end as cpc

from src_ads
where coalesce(impressions, 0) > 0

{% if is_incremental() %}
  and try_to_date(reporting_start, 'DD/MM/YYYY') >= (
      select coalesce(max(try_to_date(reporting_start, 'DD/MM/YYYY')), '1900-01-01'::date)
      from {{ this }}
  )
{% endif %}