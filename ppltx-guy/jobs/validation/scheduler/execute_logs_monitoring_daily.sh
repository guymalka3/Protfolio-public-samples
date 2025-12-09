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
# 0 8 * * *   bash ~/workspace/ppltx-guy/jobs/validation/scheduler/execute_logs_monitoring_daily.sh
# bash ~/workspace/ppltx-guy/jobs/validation/scheduler/execute_logs_monitoring_daily.sh

#  ╔═══════════════════════════════════════════╗
#  ║    Validation - Run daily log monitoring  ║
#  ╚═══════════════════════════════════════════╝

cd ~/workspace/ppltx-guy/

python3 ./jobs/validation/logs_monitoring.py subpltx-dev --etl-name log --etl-action daily

# bq head -n=5 `fp_logs.daily_logs`
# bq head -n=5 `fp_logs.logs_daily`
# bq head -n=5 `g_logs.logs_daily`

