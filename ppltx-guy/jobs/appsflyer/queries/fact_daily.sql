/*
{description}

{run_time}
*/

-- Deletes the data from the day it loads it to
DELETE FROM `{project_id}.{dataset_dst}.{table_dst}`
WHERE   {partition_att} >= '{date}';


-- Insert new data into table from inc table
INSERT INTO `{project_id}.{dataset_dst}.{table_dst}`

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
  appsflyer_id  --   SELECT *
FROM
  `{project_src}.{dataset_src}.{table_src_1}`
WHERE
  _TABLE_SUFFIX >= '{ymd}'

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
  NULL AS appsflyer_id --   SELECT *
FROM
  `{project_src}.{dataset_src}.{table_src_2}`
WHERE
  _TABLE_SUFFIX >= '{ymd}'

/*
Validation

SELECT
dt,
count(1) as cnt
FROM
`{project_id}.{dataset_dst}.{table_dst}`
GROUP BY ALL
ORDER BY 1 DESC
LIMIT 100

*/