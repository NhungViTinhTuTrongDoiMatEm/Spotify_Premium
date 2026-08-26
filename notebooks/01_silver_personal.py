# Databricks notebook source
# DBTITLE 1,01_silver_personal - PySpark Data Cleansing & Star Schema Modeling
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType
from delta.tables import DeltaTable

# 1. Thiết lập Database Schema cho tầng Silver (LOCATION tại /tmp/ cho Serverless Compute)
spark.sql("CREATE DATABASE IF NOT EXISTS spotify_silver LOCATION '/tmp/spotify_silver'")

# 2. Đọc toàn bộ các file JSON thô Bronze từ DBFS
bronze_path = "dbfs:/FileStore/spotify/bronze_raw/personal_*.json"

try:
    df_raw = spark.read.option("multiline", "true").json(bronze_path)
    print(f"📥 Đã đọc thành công dữ liệu Bronze từ: {bronze_path}")
except Exception as e:
    print(f"⚠️ Chưa tìm thấy file Bronze trên DBFS. Lỗi: {e}")
    dbutils.notebook.exit("No raw data found")

# 3. Unnest mảng JSON 'items' chứa thông tin các bài hát vừa nghe
df_exploded = df_raw.select(
    F.explode("items").alias("item"),
    F.col("ingestion_metadata.ingestion_time").alias("ingestion_time")
)

# 4. Trích xuất các trường dữ liệu phẳng (Flattening fields)
df_flattened = df_exploded.select(
    F.to_timestamp(F.col("item.played_at")).alias("played_at"),
    F.col("item.track.id").alias("track_id"),
    F.col("item.track.name").alias("track_name"),
    F.col("item.track.duration_ms").cast("integer").alias("duration_ms"),
    F.col("item.track.explicit").cast("boolean").alias("explicit"),
    F.col("item.track.popularity").cast("integer").alias("popularity"),
    F.col("item.track.album.id").alias("album_id"),
    F.col("item.track.album.name").alias("album_name"),
    F.explode("item.track.artists").alias("artist"),
    F.col("item.context.type").alias("context_type"),
    F.col("ingestion_time")
).select(
    "played_at",
    "track_id",
    "track_name",
    "duration_ms",
    "explicit",
    "popularity",
    "album_id",
    "album_name",
    F.col("artist.id").alias("artist_id"),
    F.col("artist.name").alias("artist_name"),
    "context_type",
    "ingestion_time"
)

# ==============================================================================
# 5. UPSERT (MERGE INTO) VÀO BẢNG DIM_TRACKS
# ==============================================================================
dim_tracks_df = df_flattened.select(
    "track_id", "track_name", "duration_ms", "explicit", "popularity", "album_id"
).dropDuplicates(["track_id"])

dim_tracks_table_name = "spotify_silver.dim_tracks"

if not spark.catalog.tableExists(dim_tracks_table_name):
    dim_tracks_df.write.format("delta").mode("overwrite").saveAsTable(dim_tracks_table_name)
    print("✅ Đã khởi tạo bảng Silver: spotify_silver.dim_tracks")
else:
    delta_tracks = DeltaTable.forName(spark, dim_tracks_table_name)
    delta_tracks.alias("target").merge(
        dim_tracks_df.alias("source"),
        "target.track_id = source.track_id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    print("🔄 Đã MERGE INTO (Upsert) bảng: spotify_silver.dim_tracks")

# ==============================================================================
# 6. UPSERT (MERGE INTO) VÀO BẢNG DIM_ARTISTS
# ==============================================================================
dim_artists_df = df_flattened.select("artist_id", "artist_name").dropDuplicates(["artist_id"])
dim_artists_table_name = "spotify_silver.dim_artists"

if not spark.catalog.tableExists(dim_artists_table_name):
    dim_artists_df.write.format("delta").mode("overwrite").saveAsTable(dim_artists_table_name)
    print("✅ Đã khởi tạo bảng Silver: spotify_silver.dim_artists")
else:
    delta_artists = DeltaTable.forName(spark, dim_artists_table_name)
    delta_artists.alias("target").merge(
        dim_artists_df.alias("source"),
        "target.artist_id = source.artist_id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    print("🔄 Đã MERGE INTO (Upsert) bảng: spotify_silver.dim_artists")

# ==============================================================================
# 7. MERGE INTO VÀO BẢNG FACT_STREAMS (Khử trùng lặp theo played_at + track_id)
# ==============================================================================
fact_streams_df = df_flattened.select(
    "played_at", "track_id", "artist_id", "album_id", "context_type", "ingestion_time"
).dropDuplicates(["played_at", "track_id"])

fact_streams_table_name = "spotify_silver.fact_streams"

if not spark.catalog.tableExists(fact_streams_table_name):
    fact_streams_df.write.format("delta").mode("overwrite").saveAsTable(fact_streams_table_name)
    print("✅ Đã khởi tạo bảng Silver Fact: spotify_silver.fact_streams")
else:
    delta_fact = DeltaTable.forName(spark, fact_streams_table_name)
    delta_fact.alias("target").merge(
        fact_streams_df.alias("source"),
        "target.played_at = source.played_at AND target.track_id = source.track_id"
    ).whenNotMatchedInsertAll().execute()
    print("🔄 Đã MERGE INTO khử trùng lặp vào bảng: spotify_silver.fact_streams")

print("🎉 Hoàn tất xử lý Silver Layer cho dữ liệu Cá nhân!")
