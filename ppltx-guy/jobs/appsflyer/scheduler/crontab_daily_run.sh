#!/usr/bin/env bash

#  ╔═══════════════════════════════════════════╗
#  ║    create appsflyer events - daily run    ║
#  ╚═══════════════════════════════════════════╝
#0 7 * * * bash ~/workspace/ppltx/jobs/appsflyer/scheduler/crontab_daily_run.sh

#bash ~/workspace/ppltx-guy/jobs/appsflyer/scheduler/crontab_daily_run.sh


# Run the python script according to our configuration
cd ~/workspace/ppltx-guy/


#gcloud storage create folder gs://subpltx/appsflyer/

today=$(date +"%Y-%m-%d")
echo "Generating events for date: $today"

#python3 jobs/appsflyer/generate_events.py  subpltx 0
#python3 jobs/appsflyer/generate_events_inapp.py subpltx 0

python3 ./jobs/appsflyer/generate_events.py -p subpltx-dev -b subpltx --etl-name installs --etl-action daily
python3 ./jobs/appsflyer/generate_events_inapp.py -p subpltx-dev -b subpltx --etl-name inapp --etl-action daily


#get query logs
#ls -al ~/workspace/temp/logs/ppltx-tutorial/jobs/appsflyer/

#gcloud storage objects describe gs://subpltx/appsflyer/20251003/appsflyer_installs_data.csv


#gcloud storage ls gs://subpltx/appsflyer/


