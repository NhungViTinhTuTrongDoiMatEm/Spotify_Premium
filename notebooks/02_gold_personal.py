# Databricks notebook source
# DBTITLE 1,02_gold_personal - Spark SQL KPIs & Aggregations for Lakeview Dashboard
import pyspark.sql.functions as F

# 1. Tạo Database cho tầng Gold
spark.sql("CREATE DATABASE IF NOT EXISTS spotify_gold")

# ==============================================================================
# 2. GOLD TABLE 1: TOP BÀI HÁT NGHE NHIỀU NHẤT & TỔNG SỐ PHÚT NGHE
# ==============================================================================
spark.sql("""
CREATE OR REPLACE TABLE spotify_gold.gold_top_tracks AS
SELECT 
  t.track_id, 
  t.track_name, 
  a.artist_name, 
  COUNT(*) AS total_streams,
  ROUND(SUM(t.duration_ms) / 60000.0, 2) AS total_minutes_listened,
  DENSE_RANK() OVER (ORDER BY COUNT(*) DESC) AS rank_by_streams
FROM spotify_silver.fact_streams f
JOIN spotify_silver.dim_tracks t ON f.track_id = t.track_id
JOIN spotify_silver.dim_artists a ON f.artist_id = a.artist_id
GROUP BY 
  t.track_id, 
  t.track_name, 
  a.artist_name
ORDER BY 
  total_streams DESC
LIMIT 50
""")
print("✅ Đã tạo bảng Gold: spotify_gold.gold_top_tracks")

# ==============================================================================
# 3. GOLD TABLE 2: TOP NGHỆ SĨ ĐƯỢC NGHE NHIỀU NHẤT
# ==============================================================================
spark.sql("""
CREATE OR REPLACE TABLE spotify_gold.gold_top_artists AS
SELECT 
  a.artist_id, 
  a.artist_name,
  COUNT(*) AS total_streams,
  ROUND(SUM(t.duration_ms) / 60000.0, 2) AS total_minutes_listened,
  DENSE_RANK() OVER (ORDER BY COUNT(*) DESC) AS rank_by_streams
FROM spotify_silver.fact_streams f
JOIN spotify_silver.dim_artists a ON f.artist_id = a.artist_id
JOIN spotify_silver.dim_tracks t ON f.track_id = t.track_id
GROUP BY a.artist_id, a.artist_name
ORDER BY total_streams DESC
LIMIT 50
""")
print("✅ Đã tạo bảng Gold: spotify_gold.gold_top_artists")

# ==============================================================================
# 4. GOLD TABLE 3: PHÂN BỐ KHUNG GIỜ VÀ NGÀY TRONG TUẦN (LISTENING SCHEDULE)
# ==============================================================================
spark.sql("""
CREATE OR REPLACE TABLE spotify_gold.gold_listening_schedule AS
SELECT 
  HOUR(f.played_at) AS hour_of_day,
  DAYOFWEEK(f.played_at) AS day_of_week_num,
  DATE_FORMAT(f.played_at, 'EEEE') AS day_of_week_name,
  CASE 
    WHEN HOUR(f.played_at) BETWEEN 5 AND 11 THEN 'Sáng (05h-12h)'
    WHEN HOUR(f.played_at) BETWEEN 12 AND 17 THEN 'Chiều (12h-18h)'
    WHEN HOUR(f.played_at) BETWEEN 18 AND 22 THEN 'Tối (18h-23h)'
    ELSE 'Đêm (23h-05h)'
  END AS time_slot,
  COUNT(*) AS stream_count
FROM spotify_silver.fact_streams f
GROUP BY 
  HOUR(f.played_at),
  DAYOFWEEK(f.played_at),
  DATE_FORMAT(f.played_at, 'EEEE'),
  CASE 
    WHEN HOUR(f.played_at) BETWEEN 5 AND 11 THEN 'Sáng (05h-12h)'
    WHEN HOUR(f.played_at) BETWEEN 12 AND 17 THEN 'Chiều (12h-18h)'
    WHEN HOUR(f.played_at) BETWEEN 18 AND 22 THEN 'Tối (18h-23h)'
    ELSE 'Đêm (23h-05h)'
  END
""")
print("✅ Đã tạo bảng Gold: spotify_gold.gold_listening_schedule")

# ==============================================================================
# 5. GOLD VIEW 4: BẢNG THẺ SỐ KPI TỔNG QUAN (SUMMARY SCORECARD)
# ==============================================================================
spark.sql("""
CREATE OR REPLACE VIEW spotify_gold.gold_listening_summary AS
SELECT 
  COUNT(*) AS total_streams,
  COUNT(DISTINCT f.track_id) AS unique_tracks,
  COUNT(DISTINCT f.artist_id) AS unique_artists,
  ROUND(SUM(t.duration_ms) / 3600000.0, 2) AS total_hours_listened,
  ROUND(COUNT(DISTINCT f.artist_id) * 1.0 / COUNT(*), 4) AS diversity_ratio
FROM spotify_silver.fact_streams f
JOIN spotify_silver.dim_tracks t ON f.track_id = t.track_id
""")
print("✅ Đã tạo View Gold Scorecard: spotify_gold.gold_listening_summary")

print("🎉 Hoàn tất tính toán tầng Gold cho dữ liệu Cá nhân!")
