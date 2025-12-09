#===================================
# CRONTAB CONFIGURATION
#LAST UPDATED: 21-10-31
#ENVIROMENT: Dev
#===================================
#!/usr/bin/env bash

# Exit on error
set -e
#  ╔═══════════════════════════════════════════╗
#  ║         validation - Run retry job        ║
#  ╚═══════════════════════════════════════════╝

# 0 8 * * *   bash ~/workspace/ppltx-guy/jobs/validation/scheduler/execute_retry_job.sh
# bash ~/workspace/ppltx-guy/jobs/validation/scheduler/execute_retry_job.sh

cd ~/workspace/ppltx-guy/

echo "Starting the Python retry job..."

python3 ./jobs/validation/retry_job.py subpltx-dev

echo "Retry job script finished."
