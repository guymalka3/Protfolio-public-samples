#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
---------RUN COMMAND---------
python3 ./jobs/appsflyer/generate_events.py -p guy-ppltx -b subpltx --etl-name installs --etl-action daily
python3 ./jobs/appsflyer/generate_events.py -p subpltx-dev -b subpltx --etl-name installs --etl-action daily

---------LOG TABLE---------
SELECT * FROM `guy-ppltx.fp_logs.logs_daily` ORDER BY ts DESC LIMIT 1000
------------------------
"""

import argparse
import csv
import os
import random
import uuid
import sys
from pathlib import Path
import pandas as pd
from google.cloud import storage, bigquery
import platform
from datetime import datetime, timedelta, date, time

# adapt the env to mac or windows
SCRIPT_DIR = Path(__file__).parent  # Directory of the current script
REPO_ROOT = SCRIPT_DIR.parents[1]  # Assumes script is 1 level down from repo root
# HOME_DIR = Path.home()
HOME_DIR = SCRIPT_DIR.parents[3] # Goes back to the home dir - assuming we have the same root
WORKSPACE_DIR = HOME_DIR / "workspace" # Can also change this to: WORKSPACE_DIR = SCRIPT_DIR.parents[2]
parts = list(SCRIPT_DIR.parts)

sys.path.insert(0, str(REPO_ROOT / "utilities"))
from files import ensure_directory, readFile, readJsonFile, writeFile, header, writeJsonFile

workspace_index = parts.index('workspace')
LOGS_PATH = Path(*(parts[:workspace_index + 1] + ['temp', 'logs'] + parts[workspace_index + 1:]))
ensure_directory(LOGS_PATH)
DATA_PATH = Path(*(parts[:workspace_index + 1] + ['temp/data/'] + parts[workspace_index + 1:]))
ensure_directory(DATA_PATH)
REPO_NAME = REPO_ROOT.parts[-1]
AUTH_PATH = WORKSPACE_DIR / "auth/subpltx/subpltx_sa.json"
SCRIPT_PATH_REL = Path(__file__).resolve().relative_to(REPO_ROOT).as_posix()
REL_STR = str(SCRIPT_PATH_REL)


def process_command_line(argv):
    if argv is None:
        argv = sys.argv[1:]
    # initialize the parser object:

    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--project-id", "-p" , help="Project ID")
    parser.add_argument("--bucket-name", "-b" , help="Bucket Name")
    parser.add_argument("--etl-action", choices=["daily", "hourly"], help="""The action the etl job""")
    parser.add_argument("--etl-name", help="""The name of the etl job""")
    parser.add_argument("--dry-run", help="""if True don't execute the queries""", action="store_true")
    parser.add_argument("--days-back", help="""The number of days we want to go back""", default=0)
    return parser, argparse.Namespace()


parser, flags = process_command_line(sys.argv[1:])
x = sys.argv[1:]
parser.parse_args(x, namespace=flags)

project_id = flags.project_id
etl_name = flags.etl_name
etl_action = flags.etl_action
days_back = int(flags.days_back)
bucket_name = flags.bucket_name

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

APP_NAME = "MyAwesomeGame"
APP_VERSIONS = ["1.5.1", "1.5.2", "1.5.3"]
COUNTRY_CODES = ["US", "GB", "DE", "JP", "FR", "IN", "BR"]

MEDIA_SOURCES = {
    "googleadwords_int": {
        "af_channel": "Google",
        "campaigns": ["US_Android_Search_Brand", "US_Android_UAC_Generic", "UK_iOS_Search_NonBrand"],
        "keywords": ["my awesome game", "free mobile game", "best new game 2025", None],
        "cost_model": "CPI",
        "cost_range": (1.5, 3.0)
    },
    "facebook_int": {
        "af_channel": "Facebook",
        "campaigns": ["UK_iOS_Video_Lookalike", "DE_Android_Install_Campaign", "US_Android_Retargeting"],
        "keywords": [None],
        "cost_model": "CPI",
        "cost_range": (2.0, 4.5)
    },
    "tiktok_int": {
        "af_channel": "TikTok",
        "campaigns": ["JP_iOS_Creator_Collab", "US_Android_SparkAds_Install"],
        "keywords": [None],
        "cost_model": "CPI",
        "cost_range": (1.0, 2.5)
    },
    "tiktok_int_2": {
        "af_channel": "TikTok",
        "campaigns": ["JP_iOS_Creator_Collab", "US_Android_SparkAds_Install"],
        "keywords": [None],
        "cost_model": "CPI",
        "cost_range": (1.0, 1.5)
    },
    "organic": {
        "af_channel": "Organic",
        "campaigns": ["Organic"],
        "keywords": [None],
        "cost_model": None,
        "cost_range": (0.0, 0.0)
    }
}

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    storage_client = storage.Client.from_service_account_json(AUTH_PATH)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print(f"File {source_file_name} uploaded to {destination_blob_name}.")

def generate_install_event(days_back):
    today_date = datetime.today()
    start_of_today = datetime.combine(today_date, time.min)
    total_seconds_in_day = 86400
    random_seconds = random.randint(0, total_seconds_in_day - 1)
    random_timestamp = start_of_today + timedelta(days=-days_back, seconds=random_seconds)
    install_time = random_timestamp.strftime("%Y-%m-%d %H:%M:%S")
    event_time = install_time
    event_name = "install"
    event_revenue = 0.0
    event_revenue_usd = 0.0
    event_value = "{}"
    platform_name = random.choice(["android", "ios"])
    install_app_store = "google_play" if platform_name == "android" else "itunes"
    app_version = random.choice(APP_VERSIONS)
    country_code = random.choice(COUNTRY_CODES)
    media_source_name = random.choice(list(MEDIA_SOURCES.keys()))
    source_details = MEDIA_SOURCES[media_source_name]
    af_channel = source_details["af_channel"]
    campaign = random.choice(source_details["campaigns"])
    af_keywords = random.choice(source_details["keywords"])
    af_cost_model = source_details["cost_model"]
    cost_min, cost_max = source_details["cost_range"]
    af_cost_value = round(random.uniform(cost_min, cost_max), 3) if af_cost_model else 0.0
    af_cost_currency = "USD" if af_cost_model else None
    appsflyer_id = str(uuid.uuid4())
    return {
        "install_time": install_time,
        "event_time": event_time,
        "event_name": event_name,
        "event_revenue": event_revenue,
        "event_value": event_value,
        "event_revenue_usd": event_revenue_usd,
        "af_cost_model": af_cost_model,
        "af_cost_value": af_cost_value,
        "af_cost_currency": af_cost_currency,
        "event_source": "SDK",
        "media_source": media_source_name,
        "af_channel": af_channel,
        "af_keywords": af_keywords,
        "install_app_store": install_app_store,
        "campaign": campaign,
        "country_code": country_code,
        "platform": platform_name,
        "app_version": app_version,
        "app_name": APP_NAME,
        "appsflyer_id": appsflyer_id
    }

def main(days_back):
    num_rows = int(random.uniform(12000, 15000))
    output_filename = DATA_PATH / "appsflyer_installs_data.csv"
    headers = [
        "install_time", "event_time", "event_name", "event_revenue", "event_value",
        "event_revenue_usd", "af_cost_model", "af_cost_value", "af_cost_currency",
        "event_source", "media_source", "af_channel", "af_keywords", "install_app_store",
        "campaign", "country_code", "platform", "app_version", "app_name", "appsflyer_id"
    ]
    print(f"Generating {num_rows} rows of data")
    data = []
    for _ in range(num_rows):
        data.append(generate_install_event(days_back))
    print(f"\nWriting data to:\nhead -n 5 {output_filename}")
    try:
        with open(output_filename, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            for row in data:
                writer.writerow(row)
        print(f"\nSuccessfully generated {output_filename} with {num_rows} rows.")
    except IOError as e:
        print(f"\nError: Could not write to file. {e}")

if __name__ == "__main__":
    if not flags.dry_run:
        set_log(log_dict, "start")

    main(days_back)
    ymd = (date.today() + timedelta(days=-days_back)).strftime("%Y%m%d")
    source_file_name = DATA_PATH / "appsflyer_installs_data.csv"
    destination_blob_name = f"appsflyer/{ymd}/appsflyer_installs_data.csv"
    upload_blob(bucket_name, source_file_name, destination_blob_name)
    print(f"\n\ngcloud storage objects describe gs://{bucket_name}/{destination_blob_name}\n")

    if not flags.dry_run:
        set_log(log_dict, "end")
