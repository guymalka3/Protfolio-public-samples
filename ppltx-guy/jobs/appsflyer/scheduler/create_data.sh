#!/usr/bin/env bash

#  ╔═══════════════════════════════════════════╗
#  ║    create appsflyer events - BACKFILL     ║
#  ╚═══════════════════════════════════════════╝

#bash ~/workspace/ppltx-tutorial/jobs/appsflyer/scheduler/create_data.sh
#bash ~/workspace/ppltx-guy/jobs/appsflyer/scheduler/create_data.sh

# Run the python script according to our configuration
#cd ~/workspace/ppltx-tutorial/
cd ~/workspace/ppltx-guy/


#gcloud storage create folder gs://subpltx/appsflyer/

#do
#  # -v -${i}d subtracts 'i' days from today
#  PAST_DATE=$(date -v -${i}d +"%Y-%m-%d")
#  echo "T-$i days: $PAST_DATE"
#done

for i in {0..20}
do
  TEN_DAYS_AGO=$(date -v -${i}d  +"%Y-%m-%d")
  echo "Generating events for date: $TEN_DAYS_AGO"
  python3 jobs/appsflyer/generate_events.py  subpltx $i
  python3 jobs/appsflyer/generate_events_inapp.py subpltx $i

done


#get query logs
#ls -al ~/workspace/temp/logs/ppltx-tutorial/jobs/appsflyer/

#gcloud storage objects describe gs://subpltx/appsflyer/20251003/appsflyer_installs_data.csv


#gcloud storage ls gs://subpltx/appsflyer/

