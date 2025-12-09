#===================================
# CRONTAB CONFIGURATION
#LAST UPDATED: 21-10-29
#ENVIROMENT: Dev
#===================================
#!/usr/bin/env bash

# Exit on error
set -e
#==========================
#Daily Backup at 08:00
#==========================

# m h  dom mon dow   command
# 0 8 * * *   bash ~/workspace/ppltx-guy/jobs/appsflyer/scheduler/execute_fact_daily.sh
# bash ~/workspace/ppltx-guy/jobs/appsflyer/scheduler/execute_fact_daily.sh

#  ╔═══════════════════════════════════════════╗
#  ║      appsflyer - Run daily fact etl       ║
#  ╚═══════════════════════════════════════════╝

cd ~/workspace/ppltx-guy/

python3 ./jobs/appsflyer/my_etl.py subpltx-dev --etl-name fact --etl-action daily --days-back 1

#bq head -n=5 `g_appsflyer.fact`

