# Databricks notebook source
# DBTITLE 1,02_gold_personal - Spark SQL KPIs & Aggregations for Lakeview Dashboard
import pyspark.sql.functions as F

# 1. Tạo Database cho tầng Gold
spark.sql("CREATE DATABASE IF NOT EXISTS spotify_gold")

# COMMAND ----------
# DBTITLE 1,02. GOLD TABLE 1: TOP BÀI HÁT NGHE NHIỀU NHẤT (Top Tracks)
spark.sql("""
CREATE OR REPLACE TABLE spotify_gold.gold_top_tracks AS
SELECT 
  t.track_id, 
  t.track_name, 
  CONCAT_WS(', ', COLLECT_SET(a.artist_name)) AS artist_names,
  COUNT(f.track_id) AS total_streams,
  ROUND(SUM(t.duration_ms) / 60000.0, 2) AS total_minutes_listened
FROM spotify_silver.fact_streams f
JOIN spotify_silver.dim_tracks t ON f.track_id = t.track_id
LEFT JOIN spotify_silver.bridge_track_artists b ON t.track_id = b.track_id
LEFT JOIN spotify_silver.dim_artists a ON b.artist_id = a.artist_id
GROUP BY 
  t.track_id, 
  t.track_name
ORDER BY 
  total_streams DESC
LIMIT 50
""")
print("✅ Đã cập nhật bảng Gold: spotify_gold.gold_top_tracks")

# COMMAND ----------
# DBTITLE 1,03. GOLD TABLE 2: TOP NGHỆ SĨ YÊU THÍCH (Top Artists qua Bridge Table)
spark.sql("""
CREATE OR REPLACE TABLE spotify_gold.gold_top_artists AS
SELECT 
  a.artist_id, 
  a.artist_name,
  COUNT(f.track_id) AS total_streams,
  ROUND(SUM(t.duration_ms) / 60000.0, 2) AS total_minutes_listened
FROM spotify_silver.fact_streams f
JOIN spotify_silver.dim_tracks t ON f.track_id = t.track_id
JOIN spotify_silver.bridge_track_artists b ON f.track_id = b.track_id
JOIN spotify_silver.dim_artists a ON b.artist_id = a.artist_id
GROUP BY 
  a.artist_id, 
  a.artist_name
ORDER BY 
  total_streams DESC
LIMIT 50
""")
print("✅ Đã cập nhật bảng Gold: spotify_gold.gold_top_artists")

# COMMAND ----------
# DBTITLE 1,04. GOLD TABLE 3: PHÂN BỐ KHUNG GIỜ (Tận dụng time_slot tính sẵn từ Silver)
spark.sql("""
CREATE OR REPLACE TABLE spotify_gold.gold_listening_schedule AS
SELECT 
  HOUR(f.played_at) AS hour_of_day,
  DAYOFWEEK(f.played_at) AS day_of_week_num,
  DATE_FORMAT(f.played_at, 'EEEE') AS day_of_week_name,
  f.time_slot,
  COUNT(*) AS stream_count
FROM spotify_silver.fact_streams f
GROUP BY 
  HOUR(f.played_at),
  DAYOFWEEK(f.played_at),
  DATE_FORMAT(f.played_at, 'EEEE'),
  f.time_slot
ORDER BY 
  hour_of_day ASC
""")
print("✅ Đã cập nhật bảng Gold: spotify_gold.gold_listening_schedule")

# COMMAND ----------
# DBTITLE 1,05. GOLD VIEW 4: THẺ SỐ KPI TỔNG QUAN (Summary Scorecard)
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
print("✅ Đã cập nhật View Gold Scorecard: spotify_gold.gold_listening_summary")

print("🎉 Hoàn tất tính toán tầng Gold tối ưu chuẩn Star Schema!")
