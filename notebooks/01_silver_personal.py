# Databricks notebook source
# DBTITLE 1,01. Khởi tạo Schema & Đọc Dữ liệu Bronze
import pyspark.sql.functions as F
from pyspark.sql.functions import broadcast
from delta.tables import DeltaTable

# 1. Thiết lập Database Schema cho tầng Silver
spark.sql("CREATE DATABASE IF NOT EXISTS spotify_silver")

# 2. Đọc toàn bộ file JSON thô Bronze từ Workspace Files hoặc DBFS
try:
    bronze_path = "/Workspace/spotify_raw/personal_*.json"
    df_raw = spark.read.option("multiline", "true").json(bronze_path)
    print(f"📥 Đã đọc thành công dữ liệu Bronze từ: {bronze_path}")
except Exception:
    try:
        bronze_path = "/tmp/spotify_raw/personal_*.json"
        df_raw = spark.read.option("multiline", "true").json(bronze_path)
        print(f"📥 Đã đọc thành công dữ liệu Bronze từ: {bronze_path}")
    except Exception as e:
        print(f"⚠️ Không đọc được dữ liệu Bronze từ Workspace/DBFS. Lỗi: {e}")
        dbutils.notebook.exit("No raw data found")

# 3. Unnest mảng 'items' và CACHE vào RAM để tái sử dụng tối ưu
df_exploded = df_raw.select(
    F.explode("items").alias("item"),
    F.col("ingestion_metadata.ingestion_time").alias("ingestion_time")
).cache()

print(f"⚡ Đã Cache thành công {df_exploded.count()} bản ghi vào RAM!")

# COMMAND ----------
# DBTITLE 1,02. Tách dữ liệu: dim_tracks, dim_artists & bridge_track_artists
# 1. Trích xuất Bảng DIM_TRACKS (Không bị nhân bản dòng)
dim_tracks_df = df_exploded.select(
    F.col("item.track.id").alias("track_id"),
    F.col("item.track.name").alias("track_name"),
    F.col("item.track.duration_ms").cast("integer").alias("duration_ms"),
    F.col("item.track.explicit").cast("boolean").alias("explicit"),
    F.col("item.track.album.id").alias("album_id")
).dropDuplicates(["track_id"])

# 2. Trích xuất Bảng DIM_ARTISTS (Chỉ explode riêng cho ca sĩ)
df_artists_exploded = df_exploded.select(
    F.col("item.track.id").alias("track_id"),
    F.explode("item.track.artists").alias("artist")
)

dim_artists_df = df_artists_exploded.select(
    F.col("artist.id").alias("artist_id"),
    F.col("artist.name").alias("artist_name")
).dropDuplicates(["artist_id"])

# 3. Trích xuất Bảng BRIDGE_TRACK_ARTISTS (Cầu nối quan hệ Nhiều - Nhiều)
bridge_track_artists_df = df_artists_exploded.select(
    F.col("track_id"),
    F.col("artist.id").alias("artist_id")
).dropDuplicates(["track_id", "artist_id"])

# 4. Trích xuất Bảng FACT_STREAMS (Độ mịn chuẩn: 1 Lượt nghe = 1 Dòng + Tính sẵn time_slot)
fact_streams_df = df_exploded.select(
    F.to_timestamp(F.col("item.played_at")).alias("played_at"),
    F.col("item.track.id").alias("track_id"),
    F.col("item.track.album.id").alias("album_id"),
    F.col("ingestion_time")
).withColumn(
    "time_slot",
    F.when(F.hour("played_at").between(5, 11), "Sáng (05h-12h)")
     .when(F.hour("played_at").between(12, 17), "Chiều (12h-18h)")
     .when(F.hour("played_at").between(18, 22), "Tối (18h-23h)")
     .otherwise("Đêm (23h-05h)")
).dropDuplicates(["played_at", "track_id"])

# COMMAND ----------
# DBTITLE 1,03. Upsert (MERGE INTO) với Broadcast Join vào các Bảng Silver
# --- 1. UPSERT VÀO DIM_TRACKS ---
dim_tracks_table = "spotify_silver.dim_tracks"
if not spark.catalog.tableExists(dim_tracks_table):
    dim_tracks_df.write.format("delta").mode("overwrite").saveAsTable(dim_tracks_table)
    print("✅ Đã tạo bảng Silver: dim_tracks")
else:
    delta_tracks = DeltaTable.forName(spark, dim_tracks_table)
    delta_tracks.alias("target").merge(
        broadcast(dim_tracks_df).alias("source"),
        "target.track_id = source.track_id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    print("🔄 Đã MERGE INTO (Broadcast): dim_tracks")

# --- 2. UPSERT VÀO DIM_ARTISTS ---
dim_artists_table = "spotify_silver.dim_artists"
if not spark.catalog.tableExists(dim_artists_table):
    dim_artists_df.write.format("delta").mode("overwrite").saveAsTable(dim_artists_table)
    print("✅ Đã tạo bảng Silver: dim_artists")
else:
    delta_artists = DeltaTable.forName(spark, dim_artists_table)
    delta_artists.alias("target").merge(
        broadcast(dim_artists_df).alias("source"),
        "target.artist_id = source.artist_id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    print("🔄 Đã MERGE INTO (Broadcast): dim_artists")

# --- 3. UPSERT VÀO BRIDGE_TRACK_ARTISTS ---
bridge_table = "spotify_silver.bridge_track_artists"
if not spark.catalog.tableExists(bridge_table):
    bridge_track_artists_df.write.format("delta").mode("overwrite").saveAsTable(bridge_table)
    print("✅ Đã tạo bảng Silver Cầu Nối: bridge_track_artists")
else:
    delta_bridge = DeltaTable.forName(spark, bridge_table)
    delta_bridge.alias("target").merge(
        broadcast(bridge_track_artists_df).alias("source"),
        "target.track_id = source.track_id AND target.artist_id = source.artist_id"
    ).whenNotMatchedInsertAll().execute()
    print("🔄 Đã MERGE INTO (Broadcast): bridge_track_artists")

# --- 4. MERGE INTO VÀO FACT_STREAMS (Khử trùng lặp tuyệt đối) ---
fact_table = "spotify_silver.fact_streams"
if not spark.catalog.tableExists(fact_table):
    fact_streams_df.write.format("delta").mode("overwrite").saveAsTable(fact_table)
    print("✅ Đã tạo bảng Silver Fact: fact_streams")
else:
    delta_fact = DeltaTable.forName(spark, fact_table)
    delta_fact.alias("target").merge(
        broadcast(fact_streams_df).alias("source"),
        "target.played_at = source.played_at AND target.track_id = source.track_id"
    ).whenNotMatchedInsertAll().execute()
    print("🔄 Đã MERGE INTO (Broadcast): fact_streams")

# Giải phóng bộ nhớ Cache
df_exploded.unpersist()
print("🎉 Hoàn tất xử lý Silver Layer tối ưu với Bridge Table và Caching!")
