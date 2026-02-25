{% snapshot snap_ads %}

{{
    config(
        target_schema='snapshots',
        unique_key='row_key',
        strategy='check',
        check_cols=['impressions','clicks','spent','total_conversion','approved_conversion'],
        invalidate_hard_deletes=True
    )
}}

select
    {{ dbt_utils.generate_surrogate_key([
        'ad_id',
        'campaign_id',
        'reporting_start',
        'reporting_end',
        'age',
        'gender',
        'interest1',
        'interest2',
        'interest3'
    ]) }} as row_key,

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

from {{ ref('stg_ads') }}

{% endsnapshot %}