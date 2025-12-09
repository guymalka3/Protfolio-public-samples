/*
{description}

{run_time}
*/

CREATE OR REPLACE TABLE
  `{project_id}.{dataset_dst}.{table_dst}` --  `subpltx-dev.appsflyer.fact`
PARTITION BY
{partition_att} --  date(event_time)
OPTIONS(description="{description}" )
AS

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
  `{project_src}.{dataset_src}.{table_src_1}` --  `subpltx-dev.appsflyer.appsflyer_installs_data_*`
WHERE FALSE


UNION ALL


SELECT
  DATE(event_time) AS dt,
  install_time,
  event_time,
  event_name,
  event_revenue,
  event_value,
  event_revenue_usd,
  NULL AS af_cost_model,
  NULL AS af_cost_value, --because its an integer in the other table and here string which is null for every event
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
  NULL AS appsflyer_id
FROM
  `{project_src}.{dataset_src}.{table_src_2}` --  `subpltx-dev.appsflyer.appsflyer_installs_data_*`
WHERE FALSE
