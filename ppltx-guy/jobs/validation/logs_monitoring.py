#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
---------PATH---------
C:/workspace/ppltx-guy/jobs/validation/logs_monitoring.py

---------RUN COMMAND---------
python3 ./jobs/validation/logs_monitoring.py guy-ppltx --etl-name log --etl-action daily
python3 ./jobs/validation/logs_monitoring.py subpltx-dev --etl-name log --etl-action daily

---------LOG TABLE---------
SELECT * FROM `guy-ppltx.fp_logs.logs_daily` ORDER BY ts DESC LIMIT 1000
SELECT * FROM `guy-ppltx.g_logs.logs_daily` ORDER BY ts DESC LIMIT 1000

------------------------
"""
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

from files import readFile, readJsonFile, ensureDirectory, writeFile, header, writeJsonFile
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
    parser.add_argument("project_id", help="Project ID")  # Removed choices and default; makes flexible
    parser.add_argument("--etl-action", choices=["init", "daily", "delete"], help="""The action the etl job""")
    parser.add_argument("--etl-name", help="""The name of the etl job""")
    parser.add_argument("--dry-run", help="""if True don't execute the queries""", action="store_true")
    parser.add_argument("--days-back", help="""The number of days we want to go back""",
                        default=0)
    return parser, argparse.Namespace()


parser, flags = process_command_line(sys.argv[1:])
x = sys.argv[1:]
parser.parse_args(x, namespace=flags)

project_id = flags.project_id
etl_name = flags.etl_name
etl_action = flags.etl_action
days_back = int(flags.days_back)

step_id = 0
env_type = 'daily'
log_table = f"{project_id}.g_logs.logs_{etl_action}"

client = bigquery.Client(project=project_id)

date_today = date.today()
run_time = datetime.now()
y_m_d = (date_today + timedelta(days=-days_back)).strftime("%Y-%m-%d")
ymd = y_m_d.replace("-", "")
log_message = REL_STR + " " + " ".join(x)

log_dict = {'ts': datetime.now(),
            'dt': datetime.now().strftime("%Y-%m-%d"),
            'uid': str(uuid.uuid4())[:8],
            'username': platform.node(),
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
    job.result() # Wait for the job to complete.


if not flags.dry_run:
    set_log(log_dict, "start")

# ----- Get ETL Job Config -----
etl_config_path = SCRIPT_DIR / f"config/{etl_name}_config.json"
etl_configuration = readJsonFile(etl_config_path)

# dictionary for queries
query_dict = {}
alert_columns = "raise_flag"

# Iterate all the validation groups in the conf
for alert_group_name, alerts in etl_configuration.items():
    header(alert_group_name)

    query_sql = readFile(SCRIPT_DIR / f"queries/{etl_name}_alert.sql")
    conf = alerts["alerts"]
    query_params_base = { "date" : y_m_d,
                          "run_time" : run_time,
                          "project" : project_id,
                          "job_type": etl_action
    }

    for alert_name, alert_conf in conf.items():
        print(alert_name)

        # enriched query params
        query_params = query_params_base
        query_params.update(alert_conf)

        # assign the params to the base query
        query = query_sql.format(**query_params)

        writeFile(LOGS_PATH / f"{alert_name}.sql", query)

        if not flags.dry_run:
            try:
                job_id =client.query(query)
                query_df = job_id.to_dataframe()
                query_dict[alert_name] = {}
                #get job details
                job = client.get_job(job_id)

                # union the query results
                if len(query_dict.keys()) == 1:
                    df_all = query_df
                else:
                    df_all = pd.concat([df_all, query_df], ignore_index=True)

            except Exception as s:
                msg_error = f"the error is {s}"
                header (f"Hi BI Developer we have a problem\nOpen file {str(LOGS_PATH)}/{etl_name}_error.md")
                print(msg_error)
                send_slack_alert(msg_error)
                writeFile (LOGS_PATH / f"{etl_name}_error.md", msg_error)

# if the df has values send a message
if (df_all[alert_columns]).any():
    # extract only the rows with raising_flag = True
    error_msg = "[Logs Alert]"
    # Removes days back from message to json for retry job (it does it by date)
    df_json = df_all[df_all[alert_columns]].copy()
    df_json["message"] = df_json["message"].str.replace(r'\s*--days-back\s+\d+', '', regex=True).str.strip()
    df_alert = df_json.loc[:, df_json.columns != 'message']

    msg = (f"{error_msg}\n\n*These processes hadn't run in more than N hour*\n" + format_dataframe_for_slack(df_alert)) # query_df.to_string(index=1))
    send_slack_alert(msg)
    writeFile(LOGS_PATH /  f"{etl_name}_monitoring_msg.md", msg)
    # Writes JSON log file for job Re-run config
    writeJsonFile(LOGS_PATH / f"{etl_name}_monitoring_msg.json", df_json.to_json(orient='records'))
    print(msg)
else:
    writeJsonFile(LOGS_PATH / f"{etl_name}_monitoring_msg.json", "[]")

if not flags.dry_run:
    set_log(log_dict, "end")