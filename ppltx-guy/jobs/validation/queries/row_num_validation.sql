/*
description:
{description}

run_time:
{run_time}
*/

WITH

src AS (
SELECT
{partition_atr} AS dt,
{cnt_src} AS cnt
FROM `{project_src}.{dataset_src}.{table_src}`
group by all),

dst AS(
SELECT
{partition_atr} AS dt,
{cnt_dst} AS cnt
FROM `{project_id}.{dataset_dst}.{table_dst}`
group by all)

SELECT
  src.cnt != dst.cnt OR src.cnt = 0 AS raise_flag,
  "row_num" AS kpi,
  dt,
  dst.cnt AS metric,
  src.cnt AS previous_metric,
  "{project_id}.{dataset_dst}.{table_dst}" AS table_name
FROM dst JOIN src USING (dt)
WHERE dt = '{date}'
