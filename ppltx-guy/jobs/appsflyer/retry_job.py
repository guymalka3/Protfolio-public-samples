#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
---------PATH---------
C:/workspace/ppltx-guy/jobs/gaming/retry_job.py

---------EXPLANATION---------
This job checks for log file in LOGS_PATH and uses it to config which jobs to Re-run.

---------RUN COMMAND---------
python3 ./jobs/gaming/retry_job.py subpltx-dev
python3 ./jobs/gaming/retry_job.py guy-ppltx

---------LOG TABLE---------
SELECT * FROM `guy-ppltx.fp_logs.daily_logs` ORDER BY ts DESC LIMIT 1000
------------------------
"""
import subprocess
from google.cloud import bigquery
from pathlib import Path
import os
import json
import requests
import re
import sys
import argparse
from datetime import datetime, timedelta, date
import uuid
import platform
import pandas as pd

# Path logic preserved for review
SCRIPT_DIR = Path(__file__).parent
REPO_ROOT = SCRIPT_DIR.parents[1]
WORKSPACE_DIR = SCRIPT_DIR.parents[2]
HOME_DIR = SCRIPT_DIR.parents[3]
parts = list(SCRIPT_DIR.parts)

# Added sys.path with utilities as before
sys.path.insert(0, str(REPO_ROOT / "utilities"))

from files import readFile, readJsonFile, ensureDirectory, writeFile, header
from df_to_string_table import format_dataframe_for_slack

workspace_index = parts.index('workspace')
LOGS_PATH = Path(*(parts[:workspace_index + 1] + ['temp', 'logs'] + parts[workspace_index + 1:]))
ensureDirectory(LOGS_PATH)
DATA_PATH = Path(*(parts[:workspace_index + 1] + ['temp/data/'] + parts[workspace_index + 1:]))
ensureDirectory(DATA_PATH)
username = platform.node()
REPO_NAME = REPO_ROOT.parts[-1]
AUTH_PATH = WORKSPACE_DIR / "auth" / REPO_NAME

def process_command_line(argv):
    if argv is None:
        argv = sys.argv[1:]
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("project", type=str,default= "subpltx-dev",help="path to the input file")
    parser.add_argument("--job-action", default= "daily", choices=["init", "daily", "delete"], help="""The action the  job""")
    parser.add_argument("--job-name",default= "rerun", help="""The name of the etl job""")
    parser.add_argument("-c", "--config", type=str, default= 'log_monitoring_msg.json', help="name of the log file")
    parser.add_argument("-dr", "--dry-run", action= 'store_true', help="enable dry-run output")

    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(0)

    return parser, argparse.Namespace()

parser, flags = process_command_line(sys.argv[1:])
x = sys.argv[1:]
parser.parse_args(x, namespace=flags)

project_id = flags.project
run_time = datetime.now()
job_name = flags.job_name
job_action = flags.job_action
date_today = datetime.strptime(date.today().strftime("%Y-%m-%d"), "%Y-%m-%d").date()
config_file = flags.config

step_id = 0
env_type = 'daily'
log_table = f"{project_id}.fp_logs.daily_logs"

client = bigquery.Client(project=project_id)

log_dict = {
    'ts': datetime.now(),
    'dt': datetime.now().strftime("%Y-%m-%d"),
    'uid': str(uuid.uuid4())[:8],
    'username': username,
    'job_name': job_name,
    'job_type': job_action,
    'file_name': os.path.basename(__file__),
    'step_name': 'start',
    'step_id': step_id,
    'log_type': env_type,
    'message': str(x)
}

def set_log(log_dict, step, log_table=log_table):
    log_dict['step_name'] = step
    log_dict['step_id'] += 1
    log_dict['ts'] = datetime.now()
    log_dict['dt'] = datetime.now().strftime("%Y-%m-%d")
    job = client.load_table_from_dataframe(pd.DataFrame(log_dict, index=[0]), log_table)
    job.result()

if not flags.dry_run:
    set_log(log_dict, "start")

# Configs the job Re-run from the log message
logs_message = readJsonFile(f"{LOGS_PATH}/{config_file}")

if len(logs_message) == 0:
    print(f"\nLast run was successful. There are no jobs to Re-run")
    if not flags.dry_run:
        set_log(log_dict, "end")
    sys.exit(0)

for log in logs_message:
    # Get the log parameters
    file_name = log.get("file_name")
    job_name = log.get("job_name")
    job_type = log.get("job_type")
    dt = datetime.strptime(log.get("dt"), "%Y-%m-%d").date()
    days_from_last_run = (date_today - dt).days

    header(job_name)

    print(f"\nLast run failed. Re-running the job: |*|*|*| {job_name} |*|*|*|")

    if not flags.dry_run:
        # If needed, Backfill the job from the last date it ran until today (days_back = , yesterday's data)
        if days_from_last_run > 1:
            print(f"\nJob last run was on {dt}, running Backfill...\n")

        elif days_from_last_run == 1:
            print(f"\nJob last run was on {dt}, No backfill needed.\n")

        else:
            print(f"\nJob last run was on {dt}, No run needed\n")

        for i in range(days_from_last_run, 0, -1):
            y_m_d = (date_today + timedelta(days=-i)).strftime("%Y-%m-%d")

            # List the command for the subprocess to run the jobs
            cmd = [
                "python3",
                f"{SCRIPT_DIR}/{file_name}",
                project_id,
                "--etl-name", job_name,
                "--etl-action", job_type,
                "--days-back", str(i),
            ]

            subprocess.run(cmd, shell=True)
            print(f"Job Re-run completed\n*****{y_m_d}*****\n")
    else:
        print("\nDRY RUN IS ON")

if not flags.dry_run:
    set_log(log_dict, "end")