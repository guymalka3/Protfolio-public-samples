/*
kpis_name
{kpis_name}

run_time
{run_time}
*/

WITH dau_with_lag AS (
  SELECT
    dt,
    dau,
    LAG(dau) OVER (ORDER BY dt) AS {d1}
  FROM
    `{project_id}.{dataset}.{table_id}`
)

SELECT
  -- raise_flag is true if DAU dropped more than 10% compared to previous day
  {d1} IS NOT NULL AND ABS((dau - {d1} )/dau ) > {thresh_in_percent} AS raise_flag,
  "{kpis_name}" as kpi,
  dt,
  dau as metric,
  {d1} as previous_metric,
  "{project_id}.{dataset}.{table_id}" as table_name
FROM
  dau_with_lag
ORDER BY
  dt DESC
LIMIT 1