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
# 0 8 * * *   bash ~/workspace/ppltx-guy/jobs/validation/scheduler/execute_kpis_monitoring_daily.sh
# bash ~/workspace/ppltx-guy/jobs/validation/scheduler/execute_kpis_monitoring_daily.sh

#  ╔═══════════════════════════════════════════╗
#  ║  validation - Run daily kpis_monitoring  ║
#  ╚═══════════════════════════════════════════╝

cd ~/workspace/ppltx-guy/

python3 ./jobs/validation/kpis_monitoring.py subpltx-dev --etl-name validation --etl-action daily


