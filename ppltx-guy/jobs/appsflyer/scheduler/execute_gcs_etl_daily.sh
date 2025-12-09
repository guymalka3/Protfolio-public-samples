#===================================
# CRONTAB CONFIGURATION
#LAST UPDATED: 20-11-25
#ENVIROMENT: Dev
#===================================
#!/usr/bin/env bash

# Exit on error
set -e
#==========================
#Daily Backup at 08:00
#==========================

# m h  dom mon dow   command
# 0 8 * * *   bash ~/workspace/ppltx-guy/jobs/appsflyer/scheduler/execute_gcs_etl_daily.sh
# bash ~/workspace/ppltx-guy/jobs/appsflyer/scheduler/execute_gcs_etl_daily.sh

#  ╔═══════════════════════════════════════════╗
#  ║       appsflyer - Run daily gcs etl       ║
#  ╚═══════════════════════════════════════════╝

cd ~/workspace/ppltx-guy/

python3 ./jobs/appsflyer/create_table_from_gcs.py subpltx-dev --etl-name appsflyer --etl-action daily

#bq head -n=5 `g_appsflyer.fact`
