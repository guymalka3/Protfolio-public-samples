#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
---------PATH---------
C:/workspace/ppltx-guy/jobs/appsflyer/my_etl.py

---------RUN COMMANDS ALL---------
python3 ./jobs/appsflyer/my_etl.py guy-ppltx --etl-name fact --etl-action daily
python3 jobs/appsflyer/my_etl.py subpltx-dev --etl-name fact --etl-action daily

---------LOG TABLE---------
SELECT * FROM `guy-ppltx.fp_logs.logs_daily` ORDER BY ts DESC LIMIT 1000
------------------------
"""

from google.cloud import bigquery
from pathlib import Path
import os
import sys
import argparse
from datetime import datetime, timedelta, date, timezone
import uuid
import platform
import pandas as pd

SCRIPT_DIR = Path(__file__).parent
REPO_ROOT = SCRIPT_DIR.parents[1]
WORKSPACE_DIR = SCRIPT_DIR.parents[2]
HOME_DIR = SCRIPT_DIR.parents[3]
parts = list(SCRIPT_DIR.parts)

sys.path.insert(0, str(REPO_ROOT / "utilities"))

from files import readFile, readJsonFile, ensureDirectory, writeFile, header
from df_to_string_table import format_dataframe_for_slack

try:
    from slack_alert import send_slack_alert
except ModuleNotFoundError:
    # Define a dummy function to safely skip alerts
    def send_slack_alert(text):
        pass

workspace_index = parts.index('workspace')
LOGS_PATH = Path(*(parts[:workspace_index + 1] + ['temp', 'logs'] + parts[workspace_index + 1:]))
ensureDirectory(LOGS_PATH)
DATA_PATH = Path(*(parts[:workspace_index + 1] + ['temp/data/'] + parts[workspace_index + 1:]))
ensureDirectory(DATA_PATH)
username = platform.node()
REPO_NAME = REPO_ROOT.parts[-1]
AUTH_PATH = WORKSPACE_DIR / "auth" / REPO_NAME
SCRIPT_PATH_REL = Path(__file__).resolve().relative_to(REPO_ROOT).as_posix()
REL_STR = str(SCRIPT_PATH_REL)


def process_command_line(argv):
    if argv is None:
        argv = sys.argv[1:]
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("project_id", help="Project ID")
    parser.add_argument("--etl-name", required=True, help="Name of the ETL job, or 'all' to run all jobs for the action")
    parser.add_argument("--etl-action", required=True, choices=["init", "daily", "hourly"], help="ETL action")
    parser.add_argument("--load-type", choices=["external", "native"], default="external", help="Load mode for BigQuery")
    parser.add_argument("--dry-run", action="store_true", help="If True, don't execute queries")
    parser.add_argument("--days-back", default=0, type=int, help="Days back for processing")
    return parser, argparse.Namespace()


parser, flags = process_command_line(sys.argv[1:])
x = sys.argv[1:]
parser.parse_args(x, namespace=flags)

project_id = flags.project_id
etl_name = flags.etl_name
etl_action = flags.etl_action
days_back = int(flags.days_back)
load_type = flags.load_type

step_id = 0
env_type = 'daily'
log_table = f"{project_id}.g_logs.logs_{etl_action}"

client = bigquery.Client(project=project_id)

date_today = date.today()
run_time = datetime.now()
y_m_d = (date_today + timedelta(days=-days_back)).strftime("%Y-%m-%d")
ymd = y_m_d.replace("-", "")
log_message = REL_STR + " " + " ".join(x)

log_dict = {
    'ts': datetime.now(),
    'dt': datetime.now().strftime("%Y-%m-%d"),
    'uid': str(uuid.uuid4())[:8],
    'username': username,
    'job_name': etl_name,
    'job_type': etl_action,
    'file_name': os.path.basename(__file__),
    'step_name': 'start',
    'step_id': step_id,
    'log_type': env_type,
    'message': log_message
}

def set_log(log_dict, step, log_table=log_table):
    log_dict['step_name'] = step
    log_dict['step_id'] += 1
    log_dict['ts'] = datetime.now()
    log_dict['dt'] = datetime.now().strftime("%Y-%m-%d")
    job = client.load_table_from_dataframe(pd.DataFrame(log_dict, index=[0]), log_table)
    job.result()

# ----- Get ETL Job Config -----
etl_config_path = SCRIPT_DIR / "config/etl_jobs_config.json"
etl_configuration = readJsonFile(etl_config_path)

# ---------- Encapsulate ETL logic in a function ----------
def run_etl_for_job(etl_job_name, etl_job_action, job_conf):
    # Update log_dict per job (to keep unique UIDs/logging per run)
    global sql_filename
    log_dict['job_name'] = etl_job_name
    log_dict['job_type'] = etl_job_action
    log_dict['uid'] = str(uuid.uuid4())[:8]
    log_dict['step_name'] = 'start'
    log_dict['step_id'] = 0

    if not flags.dry_run:
        set_log(log_dict, "start")

    print(f"\n>> EXECUTING: {etl_job_name.upper()} [{etl_job_action.upper()}] @ {y_m_d}")
    print(f"Description: {job_conf.get('description', '---')}")

    query_params = {
        "ymd": ymd,
        "date": y_m_d,
        "run_time": run_time,
        "project_id": project_id,
        "job_name": etl_job_name,
        "job_type": etl_job_action,
        "days_back": days_back,
        **{k: v for k, v in job_conf.items() if isinstance(v, str)}
    }

    if "sql" in job_conf:
        sql_filename = f"{job_conf['sql']}.sql"
        try:
            query_template = readFile(SCRIPT_DIR / "queries" / sql_filename)
        except Exception as ex:
            print(f"SQL file {sql_filename} not found: {ex}")
            send_slack_alert(ex)
            if not flags.dry_run:
                set_log(log_dict, "sql_not_found")
            return
        query = query_template.format(**query_params)
        writeFile(LOGS_PATH / f"{etl_job_name}_{etl_job_action}.sql", query)
    else:
        query = None

    if not flags.dry_run and query:
        try:
            print(f"Submitting query ({sql_filename}) ...")
            job_obj = client.query(query)
            job_obj.result()  # Wait for completion
            print(f"Query completed for {etl_job_name}.{etl_job_action}")
            set_log(log_dict, "success")
        except Exception as e:
            err_msg = f"[ERROR] {e}\nSee: {LOGS_PATH}/{etl_job_name}_{etl_job_action}_error.md"
            print(err_msg)
            send_slack_alert(err_msg)
            writeFile(LOGS_PATH / f"{etl_job_name}_{etl_job_action}_error.md", str(e))
            set_log(log_dict, "error")
            sys.exit(1)
    else:
        if flags.dry_run:
            print("[DRY RUN] Skipped execution.")
        else:
            print("[INFO] No query to run for this action.")

    if not flags.dry_run:
        set_log(log_dict, "end")

# --------- Main control flow: run single or all jobs ---------
if etl_name == "all":
    # Iterate over all jobs for the specified action
    ran_any = False
    for job, actions in etl_configuration["etl_jobs"].items():
        job_conf = actions.get(etl_action)
        if job_conf and job_conf.get("enabled", False):
            run_etl_for_job(job, etl_action, job_conf)
            ran_any = True
    if not ran_any:
        print(f"No enabled jobs found for action {etl_action}. Nothing to run.")
else:
    # Run single job as before
    job_config = etl_configuration["etl_jobs"].get(etl_name, {}).get(etl_action)
    if not job_config or not job_config.get("enabled", False):
        print(f"No enabled config entry for {etl_name} ({etl_action}). Exiting.")
        if not flags.dry_run:
            set_log(log_dict, "skipped")
        sys.exit(0)
    run_etl_for_job(etl_name, etl_action, job_config)

header("Script finished successfully.")
