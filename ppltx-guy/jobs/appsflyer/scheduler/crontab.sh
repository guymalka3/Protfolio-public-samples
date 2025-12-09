#  ╔═══════════════════════════════════════════╗
#  ║           appsflyer - daily run           ║
#  ╚═══════════════════════════════════════════╝
# DAILY APPSFLYER DATA GENERATOR
50 6 * * * bash ~/workspace/ppltx-guy/jobs/appsflyer/scheduler/crontab_daily_run.sh
# RUN GCS ETL JOBS
0 7 * * *   bash ~/workspace/ppltx-guy/jobs/appsflyer/scheduler/execute_gcs_etl_daily.sh
# RUN ETL DAILY
10 7 * * *   bash ~/workspace/ppltx-guy/jobs/appsflyer/scheduler/execute_all_etl_daily.sh

