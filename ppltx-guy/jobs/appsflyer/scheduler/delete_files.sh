#!/usr/bin/env bash

#  ╔═══════════════════════════════════════════╗
#  ║    create appsflyer events                ║
#  ╚═══════════════════════════════════════════╝

#bash ~/workspace/ppltx-tutorial/jobs/appsflyer/scheduler/create_data.sh

# Run the python script according to our configuration
cd ~/workspace/ppltx-tutorial/


for i in {1..10}
do
  DATE_TO_DELETE=$(date -v -${i}d  +"%Y%m%d")
  echo "delete folder: $DATE_TO_DELETE"
  gcloud storage rm --recursive gs://subpltx/appsflyer/$DATE_TO_DELETE/**

done

copy all files to ppltx-guy
cp -r ./ppltx-tutorial/jobs/appsflyer ./ppltx-guy/jobs/


## generate loop by date start and date end