#!/bin/bash
#  ╔═══════════════════════════════════════════╗
#  ║      Appsflyer - Run daily DBT Project    ║
#  ╚═══════════════════════════════════════════╝
# 0 8 * * *   bash ~/workspace/ppltx-guy/dbt/appsflyer/run_dbt.sh
# bash ~/workspace/ppltx-guy/dbt/appsflyer/run_dbt.sh

# --- 1. SETUP ---
set -o pipefail  # This ensures the exit code comes from dbt, not from tee
USER_HOME="/home/guymalka3"
PROJECT_DIR="$USER_HOME/workspace/ppltx-guy/dbt/appsflyer"
UTILS_DIR="$USER_HOME/workspace/ppltx-guy/utilities"
LOG_FILE="$PROJECT_DIR/dbt_run.log"

export PYTHONPATH="${PYTHONPATH}:${UTILS_DIR}"
cd "$PROJECT_DIR" || exit
source "../dbt_venv/bin/activate"

# --- 2. RUN ALL MODELS AND TESTS ---
# Redirect all output to log file while also viewing it on screen
dbt build 2>&1 | tee "$LOG_FILE"
DBT_STATUS=$?

# --- 3. HANDLE RESULTS & ALERTS ---
if [ $DBT_STATUS -eq 0 ]; then
    python3 -c "from slack_alert import send_slack_alert; send_slack_alert('✅ AppsFlyer: All models & tests passed!')"
else
    # Capture the logs into a variable
    RAW_ERROR=$(tail -n 15 "$LOG_FILE")

    # Use a Heredoc to pass the message into Python safely in multiline command
    python3 <<EOF
from slack_alert import send_slack_alert
import os

error_summary = """$RAW_ERROR"""
message = f"❌ AppsFlyer FAILED!\n\n*Error Summary:*\n\`\`\`\n{error_summary}\n\`\`\`"
send_slack_alert(message)
EOF
fi

# --- 4. CLEANUP ---
# Exit the virtual environment
deactivate

# Return to home directory (optional, but good practice)
cd ~/workspace/ppltx-guy/
