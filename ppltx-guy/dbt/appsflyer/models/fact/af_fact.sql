/*
dbt run --select fact
dbt compile --select fact
*/

{{ config(
    materialized='incremental',
    schema='g_dbt_af',
    partition_by={'field': 'dt', 'data_type': 'date'},
    incremental_strategy='insert_overwrite'
) }}

WITH unioned AS (
    {{ dbt_utils.union_relations(
        relations=[
            ref('af_inapp_stg'),
            ref('af_installs_stg')
        ],
        source_column_name='data_source'
    ) }}
)

{% if is_incremental() %}
    -- Incremental: Only last 7 days (partition overwrite)
    SELECT * FROM unioned
    WHERE dt >= DATE_SUB(CURRENT_DATE(), INTERVAL {{ var('lookback_days') }}  DAY)
{% else %}
    -- Full refresh: ALL data (backfill)
    SELECT * FROM unioned
{% endif %}