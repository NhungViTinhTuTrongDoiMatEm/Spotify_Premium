# Databricks notebook source
# DBTITLE 1,01. Khởi tạo Semantic Reporting Views cho Dashboard
import pyspark.sql.functions as F

# 1. Đảm bảo Database Gold đã tồn tại
spark.sql("CREATE DATABASE IF NOT EXISTS spotify_gold")

# COMMAND ----------
# DBTITLE 1,02. VIEW 1: Top 50 Bài Hát Nghe Nhiều Nhất (gold_top_tracks)
# Truy vấn siêu tốc từ bảng vật lý agg_track_metrics
spark.sql("""
CREATE OR REPLACE VIEW spotify_gold.gold_top_tracks AS
SELECT 
  track_id,
  track_name,
  artist_names,
  total_streams,
  total_minutes_listened,
  first_listened_at,
  last_listened_at
FROM spotify_gold.agg_track_metrics
ORDER BY total_streams DESC
LIMIT 50
""")
print("✅ Đã tạo View: spotify_gold.gold_top_tracks")

# COMMAND ----------
# DBTITLE 1,03. VIEW 2: Top 50 Nghệ Sĩ Yêu Thích Nhất (gold_top_artists)
# Truy vấn siêu tốc từ bảng vật lý agg_artist_metrics
spark.sql("""
CREATE OR REPLACE VIEW spotify_gold.gold_top_artists AS
SELECT 
  artist_id,
  artist_name,
  total_streams,
  total_minutes_listened,
  first_listened_at,
  last_listened_at
FROM spotify_gold.agg_artist_metrics
ORDER BY total_streams DESC
LIMIT 50
""")
print("✅ Đã tạo View: spotify_gold.gold_top_artists")

# COMMAND ----------
# DBTITLE 1,04. VIEW 3: Phân Bố Khung Giờ & Thói Quen Nghe Nhạc (gold_listening_schedule)
spark.sql("""
CREATE OR REPLACE VIEW spotify_gold.gold_listening_schedule AS
SELECT 
  hour_of_day,
  day_of_week_num,
  day_of_week_name,
  time_slot,
  stream_count
FROM spotify_gold.agg_listening_schedule
ORDER BY hour_of_day ASC
""")
print("✅ Đã tạo View: spotify_gold.gold_listening_schedule")

# COMMAND ----------
# DBTITLE 1,05. VIEW 4: Thẻ Số KPI Tổng Quan Cho Dashboard (gold_listening_summary)
spark.sql("""
CREATE OR REPLACE VIEW spotify_gold.gold_listening_summary AS
SELECT 
  COUNT(DISTINCT f.played_at, f.track_id) AS total_streams,
  COUNT(DISTINCT f.track_id) AS unique_tracks,
  COUNT(DISTINCT b.artist_id) AS unique_artists,
  ROUND(SUM(t.duration_ms) / 3600000.0, 2) AS total_hours_listened,
  ROUND(COUNT(DISTINCT b.artist_id) * 1.0 / COUNT(*), 4) AS diversity_ratio
FROM spotify_silver.fact_streams f
JOIN spotify_silver.dim_tracks t ON f.track_id = t.track_id
LEFT JOIN spotify_silver.bridge_track_artists b ON f.track_id = b.track_id
""")
print("✅ Đã tạo View: spotify_gold.gold_listening_summary")

print("🎉 Hoàn tất khởi tạo toàn bộ Semantic Views cho Lakeview Dashboard!")
