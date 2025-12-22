# Protfolio-public-samples


# Data Engineering & BI Portfolio

This repository is a collection of **data engineering and BI projects** showcasing end‑to‑end pipelines, data modeling, and monitoring.  
Projects are built primarily on **Python, SQL, and Google Cloud (GCS, BigQuery, VM, Looker Studio)**.

> ✅ All datasets in this repo are **synthetic** and generated for learning / portfolio purposes only.

---

## 📁 Projects in this Repository

### 1. AppsFlyer Campaign ETL & Analytics

**Goal:**  
Build a production‑style pipeline that ingests AppsFlyer‑like campaign data, models it in BigQuery, and exposes **campaign KPIs** (installs, revenue, CPI, ROAS, etc.) for dashboards.

**Highlights:**

- **Event generation:** Python scripts simulate daily **installs** and **in‑app events** and upload them as CSV blobs into a GCS bucket.
- **Ingestion & storage:** A generic `gcs_upload_daily.py` script:
  - Renames blobs with a `YYYYMMDD` suffix.
  - Keeps a small local staging area with 7‑day retention.
  - Creates **daily BigQuery tables** (native by default, optional external tables with `gcs_` prefix).
- **Transformation:** A config‑driven ETL runner `my_etl.py`:
  - Builds a unified `fact` table combining installs + in‑app events.
  - Aggregates into `daily_campaign_panel` – one row per `date + media_source + campaign` with all KPIs.
- **Dashboards:** Looker Studio report on top of `daily_campaign_panel` for:
  - Campaign performance, ROAS, CPI.
  - Installs, revenue, event volumes by channel.

---

### 2. Validation & Monitoring Jobs

**Goal:**  
Treat the portfolio like a real system by adding **daily checks, retries, and alerts**.

**Components:**

- `log_monitoring.py`  
  - Scans log tables to ensure each ETL step finished with `step_name = 'end'`.  
  - Writes a local summary file and sends a Slack notification with the status.

- `retry_job.py`  
  - Reads the failed jobs from the log summary.  
  - Re‑runs only the failed commands using `subprocess`, to automatically recover from transient issues.

- `kpis_monitoring.py`  
  - Runs data‑quality and freshness checks (row counts by date, non‑empty partitions, KPI thresholds).  
  - Sends alerts when something looks off (e.g., no data for today, installs drop below a defined baseline).

These jobs are scheduled on a **Google Cloud VM** via `cron`, so the whole system (ETL + validation) runs automatically every day.

---

## 🔧 Tech Stack

- **Languages:** Python, SQL  
- **Cloud:** Google Cloud Platform – GCS, BigQuery, Compute Engine (VM)  
- **Orchestration:** cron on Linux VM, config‑driven ETL, Bash wrappers  
- **Monitoring:** BigQuery log tables, Slack alerts, validation scripts  
- **BI:** Looker Studio dashboards on top of BigQuery

---

## 📌 Roadmap

This repo is designed to hold multiple portfolio projects.  
Planned additions:
- Add appsflyer-dbt review HERE
- User‑level game analytics project (cohorts, funnels, retention).  
- Additional data sources and pipelines (e.g., Mixpanel, financial/portfolio data).  

As new projects are added, this README will be updated with links and short overviews for each one.

