#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
---------RUN COMMAND---------
python3 ./jobs/appsflyer/generate_events_inapp.py -p guy-ppltx -b subpltx --etl-name inapp --etl-action daily
python3 ./jobs/appsflyer/generate_events_inapp.py -p subpltx-dev -b subpltx --etl-name inapp --etl-action daily

---------LOG TABLE---------
SELECT * FROM `guy-ppltx.fp_logs.logs_daily` ORDER BY ts DESC LIMIT 1000
------------------------
"""
import argparse
import csv
import json
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
SCRIPT_DIR = Path(__file__).parent
REPO_ROOT = SCRIPT_DIR.parents[1]
WORKSPACE_DIR = SCRIPT_DIR.parents[2]
HOME_DIR = SCRIPT_DIR.parents[3]
parts = list(SCRIPT_DIR.parts)

sys.path.insert(0, str(REPO_ROOT / "utilities"))
from files import ensure_directory, readFile, readJsonFile, writeFile, header, writeJsonFile

# get repository name
workspace_index = parts.index('workspace')
LOGS_PATH = parts[:workspace_index + 1] + ['temp', 'logs'] + parts[workspace_index + 1:]
LOGS_PATH = (Path(*LOGS_PATH))
ensure_directory(LOGS_PATH)
DATA_PATH = parts[:workspace_index + 1] + ['temp/data/'] + parts[workspace_index + 1:]
DATA_PATH = (Path(*DATA_PATH))
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

# Configuration
APP_NAME = "MyAwesomeGame"
# App versions from your sample
APP_VERSIONS = ["1.5.1", "1.5.2", "1.5.3"]

COUNTRY_CODES = ["US", "GB", "DE", "JP", "FR", "IN", "BR"]

# Platforms and their corresponding stores
PLATFORMS = {
    "ios": "itunes",
    "android": "google_play"
}

# Define different types of installs (attribution profiles)
ATTRIBUTION_PROFILES = [
    {
        "media_source": "organic",
        "af_channel": "Organic",
        "campaign": "Organic",
        "af_keywords": None
    },
    {
        "media_source": "googleadwords_int",
        "af_channel": "Google",
        "campaign": "US_Android_Search_MyAwesomeGame",
        "af_keywords": "My Awesome Game"
    },
    {
        "media_source": "facebook_int",
        "af_channel": "Facebook",
        "campaign": "US_iOS_MyAwesomeGame_Lookalike",
        "af_keywords": None
    },
    {
        "media_source": "tiktok_int",
        "af_channel": "TikTok",
        "campaign": "CA_iOS_Creator_Collab",
        "af_keywords": None
    }
]

# Define event types, their value structures, and if they generate revenue
# We use lambda functions to generate dynamic data for each event
EVENT_PROFILES = {
    "SessionStart": {
        "event_value_gen": lambda: {},
        "revenue_gen": lambda: (None, None)  # No revenue
    },
    "SessionEnd": {
        "event_value_gen": lambda: {},
        "revenue_gen": lambda: (None, None)
    },
    "TournamentStart": {
        "event_value_gen": lambda: {
            "GemsEntryFee": random.choice([50, 100, 200]),
            "CashEntryFee": "0"
        },
        "revenue_gen": lambda: (None, None)
    },
    "TournamentFinished": {
        "event_value_gen": lambda: {
            "GemsEntryFee": random.choice([50, 100, 200]),
            "CashEntryFee": "0",
            "Winnings": random.choice([0, 0, 10, 50, 150])
        },
        "revenue_gen": lambda: (None, None)
    },
    "RealCashBalance": {
        "event_value_gen": lambda: {
            "RealCashBalance": random.choice([0, 0, 5, 10, 25.50])
        },
        "revenue_gen": lambda: (None, None)
    },
    "AFRVDataMxV": {
        "event_value_gen": lambda: {
            "MaxValue": random.choice([100, 200, 500])
        },
        "revenue_gen": lambda: (None, None)
    },
    "af_purchase": {
        "event_value_gen": lambda: {
            "af_revenue": 6.99,
            "af_currency": "USD",
            "product_id": "com.app.1000gems"
        },
        "revenue_gen": lambda: (6.99, 6.99)  # Populates event_revenue
    }
}


def format_timestamp(dt_obj):
    """Formats a datetime object into the UTC string format from the sample."""
    return dt_obj.strftime("%Y-%m-%d %H:%M:%S.%f") + " UTC"


def generate_event_row(days_back):
    """Generates a single, randomized in-app event row."""

    # --- Basic Event Details ---
    # 1. Get the start of today (midnight)
    today_date = datetime.today()
    # today_date = datetime.date.today() - timedelta(days=days_back)
    start_of_today = datetime.combine(today_date, time.min)

    # 2. Define the total seconds in a day (24 * 60 * 60)
    total_seconds_in_day = 86400

    # 3. Get a random number of seconds between 0 and the total
    random_seconds = random.randint(0, total_seconds_in_day - 1)

    # 4. Add the random seconds to the start of today
    random_timestamp = start_of_today + timedelta(days=-days_back,
                                                  seconds=random_seconds)

    event_time_dt = random_timestamp.strftime("%Y-%m-%d %H:%M:%S")
    # # --- Simulate Event Time (Recent) ---
    # # Generate an event_time that happened in the last 24 hours
    # event_time_dt = datetime.datetime.utcnow() - datetime.timedelta(
    #     seconds=random.randint(1, 86400)
    # )

    # --- Simulate Install Time (Historical) ---
    # Generate an install_time that happened *before* the event
    install_time_dt = random_timestamp - timedelta(
        days=random.randint(1, 365),
        seconds=random.randint(1, 60000)
    )
    install_time_dt = install_time_dt.strftime("%Y-%m-%d %H:%M:%S")
    # --- Pick User/App Details ---
    app_name = random.choice(APP_NAME)
    app_version = random.choice(APP_VERSIONS)
    platform = random.choice(list(PLATFORMS.keys()))
    install_app_store = PLATFORMS[platform]
    country_code = random.choice(COUNTRY_CODES)

    # --- Pick Attribution Details ---
    # These details are tied to the install
    attr = random.choice(ATTRIBUTION_PROFILES)

    # --- Pick Event Details ---
    event_name = random.choice(list(EVENT_PROFILES.keys()))
    profile = EVENT_PROFILES[event_name]

    # Generate event_value JSON and revenue
    event_value_dict = profile["event_value_gen"]()
    event_value_json = json.dumps(event_value_dict)
    event_revenue, event_revenue_usd = profile["revenue_gen"]()

    # --- Assemble Row ---
    # Cost fields are None because cost is tied to the install, not the event
    row = {
        # "install_time": format_timestamp(install_time_dt),
        "install_time": install_time_dt,
        # "event_time": format_timestamp(event_time_dt),
        "event_time": event_time_dt,
        "event_name": event_name,
        "event_value": event_value_json,
        "event_revenue": event_revenue,
        "event_revenue_usd": event_revenue_usd,
        "af_cost_currency": None,
        "af_cost_value": None,
        "media_source": attr["media_source"],
        "campaign": attr["campaign"],
        "af_channel": attr["af_channel"],
        "country_code": country_code,
        "event_source": "SDK",
        "af_keywords": attr["af_keywords"],
        "install_app_store": install_app_store,
        "platform": platform,
        "app_version": app_version,
        "app_name": app_name
    }

    return row


def main(days_back):
    """Main function to generate data and write to CSV."""

    # try:
    #     num_rows = int(input("How many rows of mock data do you want to generate? "))
    # except ValueError:
    #     print("Invalid input. Defaulting to 100 rows.")
    #     num_rows = 100

    num_rows = int(random.uniform(22000, 25000))

    output_filename = DATA_PATH / "appsflyer_inapp_data.csv"

    # Get headers directly from your schema
    headers = [
        "install_time", "event_time", "event_name", "event_value",
        "event_revenue", "event_revenue_usd", "af_cost_currency",
        "af_cost_value", "media_source", "campaign", "af_channel",
        "country_code", "event_source", "af_keywords", "install_app_store",
        "platform", "app_version", "app_name"
    ]

    print(f"Generating {num_rows} event rows...")

    try:
        with open(output_filename, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()

            for _ in range(num_rows):
                writer.writerow(generate_event_row(days_back))

        print(f"\nSuccessfully generated {output_filename} with {num_rows} rows.")

    except IOError as e:
        print(f"\nError: Could not write to file. {e}")




def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # bucket_name = "yppltx-app"
    # The path to your file to upload
    # source_file_name = "/Users/liadlivne/workspace/bidev/tech/gs/users_data.csv"
    # The ID of your GCS object
    # destination_blob_name = "gcs-daily-etl/20250311/user_data.csv"

    storage_client = storage.Client.from_service_account_json(AUTH_PATH)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    # Optional: set a generation-match precondition to avoid potential race conditions
    # and data corruptions. The request to upload is aborted if the object's
    # generation number does not match your precondition. For a destination
    # object that does not yet exist, set the if_generation_match precondition to 0.
    # If the destination object already exists in your bucket, set instead a
    # generation-match precondition using its generation number.
    # generation_match_precondition = 0

    # blob.upload_from_filename(source_file_name, if_generation_match=generation_match_precondition)
    blob.upload_from_filename(source_file_name)

    print(
        f"File {source_file_name} uploaded to {destination_blob_name}."
    )

if __name__ == "__main__":
    if not flags.dry_run:
        set_log(log_dict, "start")

    main(days_back)
    ymd = (date.today() + timedelta(days=-days_back)).strftime("%Y%m%d")
    # ymd = datetime.datetime.now().strftime("%Y%m%d")
    # Upload the generated file to GCS
    # bucket_name = "subpltx"
    source_file_name = DATA_PATH / "appsflyer_inapp_data.csv"
    destination_blob_name = f"appsflyer/{ymd}/appsflyer_inapp_data.csv"
    upload_blob(bucket_name, source_file_name, destination_blob_name)
    print(f"\n\ngcloud storage objects describe gs://{bucket_name}/{destination_blob_name}\n")

    if not flags.dry_run:
        set_log(log_dict, "end")