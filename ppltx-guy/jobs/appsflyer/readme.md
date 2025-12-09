# Appsflyer
Appsflyer is a leading mobile attribution and marketing analytics platform that enables businesses
to accurately measure the performance of their mobile marketing campaigns, optimize user acquisition,
and analyze user engagement across various channels. The platform provides robust tools for tracking installs, 
in-app events, and user journeys, empowering organizations to evaluate ROI and user quality with precision. 
With features such as deep linking, fraud protection, and unified analytics, 
Appsflyer offers comprehensive solutions for campaign attribution, data-driven decision-making, 
and privacy compliance. For detailed technical guidance and integration support, 
refer to the official [AppsFlyer documentation](https://dev.appsflyer.com/hc) and [Help Center](https://support.appsflyer.com/hc/en-us).
***
## To Do / Improve
- Generate daily dashboard in Looker Studio - Define the KPIs
- Monitoring & Alerts - set up alerts for anomalies in key metrics
- check params - max bad records? skip leading rows? null marker?
- fix for adding column to table

***
# Description
1. This job generates data (events) to GCS Bucket.

2. This job takes daily/hourly blob and downloads it to bigquery (external or native) table,
then aggregates all to one `fact` table.

Default - Native table is set by the flag - `--load-type`

Log table - `g_logs.logs_daily` / `g_logs.logs_hourly` - depends on the `--etl-action`
***
# Commands
1. Create schemas
```dtd
CREATE SCHEMA IF NOT EXISTS `g_appsflyer`
CREATE SCHEMA IF NOT EXISTS `g_logs`
```
2. Run GCS ETL once to have the schemas for the fact table
```dtd
python3 ./jobs/appsflyer/create_table_from_gcs.py subpltx-dev --etl-name appsflyer --etl-action daily
```
3. Run all init - To create the fact table and the daily panel
```dtd
python3 ./jobs/appsflyer/my_etl.py subpltx-dev --etl-name all --etl-action init
```
## Daily Run
```dtd
#  ╔═══════════════════════════════════════════╗
#  ║           appsflyer - daily run           ║
#  ╚═══════════════════════════════════════════╝
# DAILY APPSFLYER DATA GENERATOR
50 6 * * * bash ~/workspace/ppltx-guy/jobs/appsflyer/scheduler/crontab_daily_run.sh
# RUN GCS ETL JOBS
0 7 * * *   bash ~/workspace/ppltx-guy/jobs/appsflyer/scheduler/execute_gcs_etl_daily.sh
# RUN ETL DAILY
10 7 * * *   bash ~/workspace/ppltx-guy/jobs/appsflyer/scheduler/execute_all_etl_daily.sh
```
## Backfills - 20 days
Run GCS ETL Backfill if needed
```bash
bash ~/workspace/ppltx-guy/jobs/appsflyer/scheduler/backfill_gcs_etl_daily.sh
```
Run Fact ETL Backfill if needed
```bash
bash ~/workspace/ppltx-guy/jobs/appsflyer/scheduler/backfill_fact_daily.sh
```

## Validation
```dtd
SELECT
dt,
COUNT(1) AS cnt
FROM
  `subpltx-dev.g_appsflyer.fact`
  GROUP BY ALL
  ORDER BY dt DESC
LIMIT
  1000
```
the same as
```dtd
SELECT
DATE(event_time) as dt,
COUNT(1) AS cnt
FROM
  `subpltx-dev.g_appsflyer.appsflyer_*`
  GROUP BY ALL
  ORDER BY dt DESC
LIMIT
  1000
```
the same as
```dtd
SELECT
dt,
sum(t_events) as cnt
FROM
  `subpltx-dev.g_appsflyer.daily_campaign_panel`
group by all
order by dt desc
```

# Dashboard - [Link](https://lookerstudio.google.com/s/pO5MBm7VVco)