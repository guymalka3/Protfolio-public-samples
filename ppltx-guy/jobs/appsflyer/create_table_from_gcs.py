#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
---------PATH---------
C:/workspace/ppltx-guy/jobs/appsflyer/create_table_from_gcs.py

---------RUN COMMANDS---------
python3 ./jobs/appsflyer/create_table_from_gcs.py guy-ppltx --etl-name appsflyer --etl-action init --dry-run
python3 ./jobs/appsflyer/create_table_from_gcs.py subpltx-dev --etl-name appsflyer --etl-action daily --dry-run

---------LOG TABLE---------
SELECT * FROM `guy-ppltx.fp_logs.logs_daily` ORDER BY ts DESC LIMIT 1000
------------------------
"""

from google.cloud import bigquery, storage
import os
import sys
import argparse
from datetime import datetime, timedelta, date, timezone
import uuid
import platform
import pandas as pd
from pathlib import Path
import re
from typing import Literal

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
    parser.add_argument("--etl-name", required=True, help="Name of the ETL job")
    parser.add_argument("--etl-action", required=True, choices=["init", "daily", "hourly"], help="ETL action")
    parser.add_argument("--load-type", choices=["external", "native"], default="native", help="Load mode for BigQuery")
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
env_type = etl_action
log_table = f"{project_id}.g_logs.logs_{etl_action}"

client = bigquery.Client(project=project_id)
gcs_client = storage.Client()

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
etl_config_path = SCRIPT_DIR / "config/gcs_etl_config.json"
etl_configuration = readJsonFile(etl_config_path)

# --------- Define Functions --------
def sanitize_table_name(blob_name):
    base_name = Path(blob_name).stem  # filename without extension
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', base_name)  # replace invalid chars with underscores
    return sanitized


def infer_file_type(blob_name: str) -> str:
    extension_map = {
        "json": "json",
        "ndjson": "json",
        "csv": "csv",
        "parquet": "parquet",
        "avro": "avro",
        "orc": "orc",
    }
    try:
        ext = Path(blob_name).suffix.lstrip(".").lower()
        if ext in extension_map:
            return extension_map[ext]
        else:
            raise ValueError(f"Unsupported file extension '{ext}' for blob: {blob_name}")
    except Exception as ex:
        msg = f"Failed to infer file type from blob '{blob_name}': {ex}"
        print(msg)
        send_slack_alert(msg)
        raise


def download_blob_to_local(blob, blob_name, file_type):
    try:
        """Download a single blob to the specified local path."""
        local_file = DATA_PATH / f"{blob_name}_{ymd}.{file_type}"
        blob.download_to_filename(str(local_file))
        return True
    except Exception as ex:
        msg = f"Failed to download blob {blob.name}: {ex}"
        print(msg)
        send_slack_alert(msg)
        return False


def delete_old_backups(retention_days=7):
    """Delete files older than retention_days in local_dir."""
    cutoff_date = datetime.now().date() - timedelta(days=retention_days)
    date_pattern = re.compile(r'_(\d{8})')  # matches underscore followed by 8 digits
    counter = 0

    for f in DATA_PATH.glob("*"):
        if f.is_file():
            match = date_pattern.search(f.name)
            if match:
                file_date_str = match.group(1)
            try:
                file_date = datetime.strptime(file_date_str, "%Y%m%d").date()
                if file_date < cutoff_date:
                    f.unlink()
                    counter += 1
                    print(f"Deleted old backup file: {f}")
            except (ValueError, UnboundLocalError) as ex:
                err_msg = f"Failed to delete old backup file: {f}, error: {ex}"
                send_slack_alert(err_msg)
                continue # invalid date format in filename / file with no date format, skip
    print(f"Deleted total: {counter} files")


def create_external_table(blob_name, gcs_uri, file_type):

    dataset_id = job_conf.get("dataset_id", etl_name)
    table_ref = f"{project_id}.{dataset_id}.gcs_{blob_name}_{ymd}"

    if file_type == "json":
        external_config = bigquery.ExternalConfig("NEWLINE_DELIMITED_JSON")
    elif file_type == "csv":
        external_config = bigquery.ExternalConfig("CSV")
        external_config.options.skip_leading_rows = 1
        external_config.options.encoding = "UTF-8"
        external_config.options.quote_character = '"'
    elif file_type == "parquet":
        external_config = bigquery.ExternalConfig("PARQUET")
    elif file_type == "avro":
        external_config = bigquery.ExternalConfig("AVRO")
    elif file_type == "orc":
        external_config = bigquery.ExternalConfig("ORC")
    else:
        msg = f"Unsupported file type for external table: {file_type}"
        send_slack_alert(msg)
        raise ValueError(msg)

    external_config.source_uris = [gcs_uri]
    external_config.autodetect = True

    table = bigquery.Table(table_ref)
    table.external_data_configuration = external_config
    table_job = client.create_table(table, exists_ok=True)
    print(f"Created external table: `gcs_{blob_name}_{ymd}` with {table_job.num_rows} rows.")


def create_native_table(blob_name, gcs_uri, file_type):

    dataset_id = job_conf.get("dataset_id", etl_name)
    table_ref = f"{project_id}.{dataset_id}.{blob_name}_{ymd}"

    table_config = bigquery.LoadJobConfig()
    table_config.autodetect = True
    table_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    if file_type == "json":
        table_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    elif file_type == "csv":
        table_config.source_format = bigquery.SourceFormat.CSV
        table_config.skip_leading_rows = 1
        table_config.encoding = "UTF-8"
        table_config.quote_character = '"'
    elif file_type == "parquet":
        table_config.source_format = bigquery.SourceFormat.PARQUET
    elif file_type == "avro":
        table_config.source_format = bigquery.SourceFormat.AVRO
    elif file_type == "orc":
        table_config.source_format = bigquery.SourceFormat.ORC
    else:
        msg = f"Unsupported file type for native table: {file_type}"
        send_slack_alert(msg)
        raise ValueError(msg)

    load_job = client.load_table_from_uri(
        gcs_uri,
        table_ref,
        job_config=table_config
    )
    load_job.result()

    table = client.get_table(table_ref)
    print(f"Created native table: `{blob_name}_{ymd}` with {table.num_rows} rows.")

# ---------- Run ETL----------
if not flags.dry_run:
    set_log(log_dict, "start")

for etl_group_name, group_conf in etl_configuration.items():
    header(etl_group_name)

    for job, job_conf in group_conf[f"{etl_name}"].items():
        if not job_conf["enabled"] or job != etl_action:
            continue
        
        print(f">> EXECUTING: {etl_name.upper()} [{etl_action.upper()}] @ {y_m_d}")
        print(f"Description: {job_conf.get('description', '---')}")

        bucket_name = job_conf["bucket_name"]
        prefix = f"{etl_name}/{ymd}"
        blobs = list(gcs_client.list_blobs(bucket_name, prefix=prefix))

        if not blobs:
            print(f"[INFO] No blobs found with prefix {prefix}")
            continue

        if not flags.dry_run:
            delete_old_backups(retention_days=7)

        for blob in blobs:
            
            try:
                blob_file_type = infer_file_type(blob.name)
                sanitized_blob_name = sanitize_table_name(blob.name)
                blob_gcs_uri = f"gs://{bucket_name}/{blob.name}"
    
                if not flags.dry_run:
    
                    download_blob_to_local(blob, sanitized_blob_name, blob_file_type)
                    print(f"\nDownloaded {blob.name} to {DATA_PATH / (sanitized_blob_name + '_' + ymd + '.' + blob_file_type)}")
    
                    if load_type == "external":
                        create_external_table(sanitized_blob_name, blob_gcs_uri, blob_file_type)
                    else:
                        create_native_table(sanitized_blob_name, blob_gcs_uri, blob_file_type)
    
                else:
                    print(f"\n[DRY RUN] Would download {blob.name} to {DATA_PATH / (sanitized_blob_name + '_' + ymd + '.' +blob_file_type)}")
                    print(f"[DRY RUN] Would create table {sanitized_blob_name} with {blob_gcs_uri}")
            except Exception as e:
                print(f"\n[ERROR] {e}")
                send_slack_alert(e)

if not flags.dry_run:
    set_log(log_dict, "end")

header("Script finished successfully.")
