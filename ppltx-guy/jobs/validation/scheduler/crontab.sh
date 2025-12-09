#  ╔═══════════════════════════════════════════╗
#  ║          Monitoring & Validation          ║
#  ╚═══════════════════════════════════════════╝
# RUN DAILY VALIDATIONS
20 7 * * *   bash ~/workspace/ppltx-guy/jobs/validation/scheduler/execute_kpis_monitoring_daily.sh
# RUN DAILY LOG MONITORING
30 7 * * * bash ~/workspace/ppltx-guy/jobs/validation/scheduler/execute_logs_monitoring_daily.sh
# RUN DAILY RETRY JOB
40 7 * * *   bash ~/workspace/ppltx-guy/jobs/validation/scheduler/execute_retry_job.sh