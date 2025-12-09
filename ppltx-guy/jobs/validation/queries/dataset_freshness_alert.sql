/*
kpis_name
{kpis_name}

last_modified_time: {project_src}.{dataset_id}.{table_id}

run_time
{run_time}
*/



SELECT
  timestamp_diff(current_timestamp(),TIMESTAMP_MILLIS(last_modified_time ), hour) > {thresh_in_hours} as raise_flag,
  "{kpis_name} (hours)" AS kpi,
  FORMAT_DATE('%Y-%m-%d',TIMESTAMP_MILLIS(last_modified_time)) AS dt,
  timestamp_diff(current_timestamp(),TIMESTAMP_MILLIS(last_modified_time ), hour) as metric,
  TIMESTAMP_MILLIS(last_modified_time) as previous_metric,
  "{project_src}.{dataset_id}.{table_id}" as table_name
FROM `{project_src}.{dataset_id}`.__TABLES__
WHERE table_id = '{table_id}'
