/*
kpis_name
{kpis_name}

run_time
{run_time}
*/

WITH installs_daily_with_lag AS (
  SELECT
    *,
    LAG(installs) OVER (ORDER BY install_dt) AS {d1}
FROM
  `{project_id}.{dataset}.{table_id}`
)

SELECT
  -- raise_flag is true if installs dropped more than 10% compared to previous day
  {d1} IS NOT NULL AND ABS((installs - {d1} )/installs ) > {thresh_in_percent} AS raise_flag,
  "{kpis_name}" as kpi,
  install_dt as dt,
  installs as metric,
  {d1} as previous_metric,
  "{project_id}.{dataset}.{table_id}" as table_name
FROM
  installs_daily_with_lag
ORDER BY
  install_dt DESC
LIMIT 1