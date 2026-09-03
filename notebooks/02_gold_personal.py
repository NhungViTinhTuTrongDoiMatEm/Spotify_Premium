# Databricks notebook source
# DBTITLE 1,01. Khởi tạo Database cho tầng Gold
import pyspark.sql.functions as F

# 1. Tạo Database cho tầng Gold
spark.sql("CREATE DATABASE IF NOT EXISTS spotify_gold")

# COMMAND ----------
# DBTITLE 1,02. AGG TABLE 1: Tổng hợp Số liệu 100% Bài Hát (agg_track_metrics)
# Bảng vật lý lưu toàn diện mọi bài hát bạn từng nghe, không giới hạn Top 50
spark.sql("""
CREATE OR REPLACE TABLE spotify_gold.agg_track_metrics AS
SELECT 
  t.track_id, 
  t.track_name, 
  CONCAT_WS(', ', COLLECT_SET(a.artist_name)) AS artist_names,
  COUNT(f.track_id) AS total_streams,
  ROUND(SUM(t.duration_ms) / 60000.0, 2) AS total_minutes_listened,
  MIN(f.played_at) AS first_listened_at,
  MAX(f.played_at) AS last_listened_at
FROM spotify_silver.fact_streams f
JOIN spotify_silver.dim_tracks t ON f.track_id = t.track_id
LEFT JOIN spotify_silver.bridge_track_artists b ON t.track_id = b.track_id
LEFT JOIN spotify_silver.dim_artists a ON b.artist_id = a.artist_id
GROUP BY 
  t.track_id, 
  t.track_name
""")
print("✅ Đã cập nhật bảng Gold Vật lý: agg_track_metrics (Toàn bộ bài hát)")

# COMMAND ----------
# DBTITLE 1,03. AGG TABLE 2: Tổng hợp Số liệu 100% Nghệ Sĩ (agg_artist_metrics)
# Bảng vật lý lưu toàn diện mọi nghệ sĩ bạn từng nghe
spark.sql("""
CREATE OR REPLACE TABLE spotify_gold.agg_artist_metrics AS
SELECT 
  a.artist_id, 
  a.artist_name,
  COUNT(f.track_id) AS total_streams,
  ROUND(SUM(t.duration_ms) / 60000.0, 2) AS total_minutes_listened,
  MIN(f.played_at) AS first_listened_at,
  MAX(f.played_at) AS last_listened_at
FROM spotify_silver.fact_streams f
JOIN spotify_silver.dim_tracks t ON f.track_id = t.track_id
JOIN spotify_silver.bridge_track_artists b ON f.track_id = b.track_id
JOIN spotify_silver.dim_artists a ON b.artist_id = a.artist_id
GROUP BY 
  a.artist_id, 
  a.artist_name
""")
print("✅ Đã cập nhật bảng Gold Vật lý: agg_artist_metrics (Toàn bộ nghệ sĩ)")

# COMMAND ----------
# DBTITLE 1,04. AGG TABLE 3: Phân bố Khung giờ & Ngày trong tuần (agg_listening_schedule)
spark.sql("""
CREATE OR REPLACE TABLE spotify_gold.agg_listening_schedule AS
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
print("✅ Đã cập nhật bảng Gold Vật lý: agg_listening_schedule")

print("🎉 Hoàn tất tính toán các Bảng Vật Lý Tầng Gold (Cumulative Aggregation Tables)!")
