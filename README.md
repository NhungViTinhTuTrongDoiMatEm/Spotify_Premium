# 🎵 Personal Spotify Analytics Lakehouse

A Medallion Architecture Data Lakehouse built on **Databricks** and automated with **GitHub Actions** to continuously ingest, clean, and analyze personal Spotify listening history.

---

## 🏗 Architecture & Medallion Layers

- **Bronze Layer (Raw Ingestion)**: Automated ingestion of Recently Played tracks via Spotify Web API.
- **Silver Layer (Cleaned & Star Schema)**: PySpark Data Cleansing, Unnesting, and Deduplication via `MERGE INTO` (`dim_tracks`, `dim_artists`, `fact_streams`).
- **Gold Layer (KPIs & Aggregations)**: Spark SQL Window Functions calculating Top Tracks, Top Artists, Listening Schedules, and Summary Scorecards.
- **Automation**: GitHub Actions workflow (`.github/workflows/spotify_ingest_personal.yml`) running every 6 hours 24/7.
- **Visualization**: Databricks Lakeview Interactive Dashboard.

---

## 📁 Repository Structure

```text
Spotify_Premium/
├── .github/
│   └── workflows/
│       └── spotify_ingest_personal.yml   # 6-hour cron automation
├── config/
│   └── config.json                       # Global configurations
├── docs/
│   └── setup_guide.md                    # Setup & Deployment Guide
├── helper/
│   └── get_spotify_refresh_token.py      # OAuth2 Refresh Token generator
├── notebooks/                            # Databricks Notebooks
│   ├── 01_silver_personal.py            # PySpark Silver Layer (Star Schema & Merge)
│   └── 02_gold_personal.py              # Spark SQL Gold Layer (KPIs & Metrics)
├── src/
│   ├── ingest_personal.py               # Spotify Recently Played API Ingestion
│   └── ingest_charts.py                 # Daily Top 50 Regional Charts Ingestion
├── .env.example
├── .gitignore
├── requirements.txt
└── README.md
```

---

## 🚀 Quick Start

1. Install Python dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Generate Spotify Refresh Token:
   ```bash
   python helper/get_spotify_refresh_token.py
   ```
3. Test personal ingestion locally:
   ```bash
   python src/ingest_personal.py
   ```
4. Read the full deployment guide in [`docs/setup_guide.md`](docs/setup_guide.md).
