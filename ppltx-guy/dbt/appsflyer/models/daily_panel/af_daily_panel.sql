/*
dbt run --select daily_panel
dbt compile --select daily_panel
*/

{{ config(
    materialized='incremental',
    schema='g_dbt_af',
    partition_by={'field': 'dt', 'data_type': 'date'},
    incremental_strategy='insert_overwrite'
) }}

WITH daily AS (

    SELECT
    campaign,
    dt,
    MAX(af_channel) AS media_source,
    count(1) as t_events,
    {{ sum_total_events( column_name = 'event_name',
    events=['install', 'af_purchase', 'SessionStart', 'SessionEnd', 'TournamentStart', 'TournamentFinished'])}},
    {{ sum_total_events( column_name = 'platform',
    events=['ios', 'android'])}},
    {{ sum_total_events( column_name = 'af_channel',
    events=['Facebook', 'Google', 'TikTok', 'Organic' ])}},
    ROUND(SUM(event_revenue)) AS revenue,
    ROUND(SUM(af_cost_value)) AS cost,
    ROUND(SUM(event_revenue_usd) / NULLIF(SUM(af_cost_value), 0), 2) AS roas,
    MAX(data_source) AS data_source

    FROM {{ ref('af_fact') }}
    GROUP BY ALL
    ORDER BY 2 DESC
)

{% if is_incremental() %}
    -- Incremental: Only last 7 days (partition overwrite)
    SELECT * FROM daily
    WHERE dt >= DATE_SUB(CURRENT_DATE(), INTERVAL {{ var('lookback_days') }} DAY)
{% else %}
    -- Full refresh: ALL data (backfill)
    SELECT * FROM daily
{% endif %}