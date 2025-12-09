/*
kpis_name
{kpis_name}

run_time
{run_time}
*/

SELECT
  -- raise_flag is true if LAST ACTIVE DATE is N days ago
  DATE_DIFF(CURRENT_DATE(), last_active_dt, DAY) > {thresh_in_days} AS raise_flag,
  CONCAT("{kpis_name}:"," ", user_id) as kpi,
  CURRENT_DATE() as dt,
  DATE_DIFF(CURRENT_DATE(), last_active_dt, DAY) as metric,
  last_active_dt as previous_metric,
  "{project_id}.{dataset}.{table_id}" as table_name
FROM
  {project_id}.{dataset}.{table_id} as table_name
WHERE
  DATE_DIFF(CURRENT_DATE(), last_active_dt, DAY) > {thresh_in_days}