/*
dbt test --select af_data_quality
*/

{{ config(
    error_if = ">0",
    warn_if = ">0",
) }}


with raw_installs as (
    select
        DATE(event_time) as dt,
        count(*) as row_count
    from {{ source('appsflyer_pl', 'installs_raw') }}
    where DATE(event_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL {{ var('lookback_days') }}  DAY)
    group by all
),
raw_inapp as (
    select
        DATE(event_time) as dt,
        count(*) as row_count
    from {{ source('appsflyer_pl', 'inapp_raw') }}
    where DATE(event_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL {{ var('lookback_days') }}  DAY)
    group by all
),
fact as (
    select
        dt,
        count(*) as row_count
    from {{ ref('af_fact') }}
    where dt >= DATE_SUB(CURRENT_DATE(), INTERVAL {{ var('lookback_days') }}  DAY)
    group by all
),
daily_panel as (
    select
        dt,
        sum(t_events) as row_count
    from {{ ref('af_daily_panel') }}
    where dt >= DATE_SUB(CURRENT_DATE(), INTERVAL {{ var('lookback_days') }}  DAY)
    group by all
),
final_comparison as (
    select
        coalesce(raw_installs.dt, raw_inapp.dt, fact.dt, daily_panel.dt) as dt,
        coalesce(raw_installs.row_count, 0) + coalesce(raw_inapp.row_count, 0) as ext_rows,
        coalesce(fact.row_count, 0) as fact_rows,
        coalesce(daily_panel.row_count, 0) as daily_panel_rows
    from raw_installs
    full outer join raw_inapp using (dt)
    full outer join fact using (dt)
    full outer join daily_panel using (dt)
)

select * from final_comparison
-- Test fails if any of the three stages don't match
where
    abs(coalesce(ext_rows, 0) - coalesce(fact_rows, 0)) > 0
    or abs(coalesce(fact_rows, 0) - coalesce(daily_panel_rows, 0)) > 0
