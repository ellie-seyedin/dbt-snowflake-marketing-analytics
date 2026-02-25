select *
from {{ ref('fct_ads_performance') }}
where coalesce(impressions, 0) < 0
   or coalesce(clicks, 0) < 0
   or coalesce(spent, 0) < 0
   or coalesce(total_conversion, 0) < 0
   or coalesce(approved_conversion, 0) < 0