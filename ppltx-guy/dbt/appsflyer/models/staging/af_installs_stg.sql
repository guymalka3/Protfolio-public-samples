/*
dbt run --select staging
dbt compile --select staging
*/

{{ config(
    materialized='view',
    schema='g_dbt_af',
) }}

{#  #}

SELECT
  DATE(event_time) AS dt,
  install_time,
  event_time,
  event_name,
  event_revenue,
  event_value,
  event_revenue_usd,
  af_cost_model,
  af_cost_value,
  af_cost_currency,
  event_source,
  media_source,
  af_channel,
  af_keywords,
  install_app_store,
  campaign,
  country_code,
  platform,
  app_version,
  app_name,
  appsflyer_id
FROM
  {{ source('appsflyer_pl', 'installs_raw') }}