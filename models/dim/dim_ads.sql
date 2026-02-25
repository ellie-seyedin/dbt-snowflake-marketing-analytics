{{ config(materialized='table') }}

WITH src_ads AS (
    SELECT * FROM {{ ref('src_ads') }}
)

SELECT DISTINCT
    AD_ID AS ad_id,
    CAMPAIGN_ID AS campaign_id,
    FB_CAMPAIGN_ID AS fb_campaign_id,
    TRIM(AGE) AS age,
    UPPER(TRIM(GENDER)) AS gender,
    INTEREST1 AS interest1,
    INTEREST2 AS interest2,
    INTEREST3 AS interest3
FROM src_ads