# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
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

# 3. Đọc dữ liệu Album Catalog (Tracklist đầy đủ) nếu đã cào
has_album_catalog = False
df_albums_raw = None
try:
    album_catalog_path = "/Workspace/spotify_raw/album_catalog_*.json"
    df_albums_raw = spark.read.option("multiline", "true").json(album_catalog_path)
    has_album_catalog = True
    print(f"📥 Đã đọc thành công Album Catalog từ: {album_catalog_path}")
except Exception:
    try:
        album_catalog_path = "/tmp/spotify_raw/album_catalog_*.json"
        df_albums_raw = spark.read.option("multiline", "true").json(album_catalog_path)
        has_album_catalog = True
        print(f"📥 Đã đọc thành công Album Catalog từ: {album_catalog_path}")
    except Exception:
        print("ℹ️ Chưa có file Album Catalog, sử dụng dữ liệu album từ lịch sử nghe.")

# 4. Unnest mảng 'items' từ lịch sử nghe cá nhân và CACHE vào RAM
df_exploded = df_raw.select(
    F.explode("items").alias("item"),
    F.col("ingestion_metadata.ingestion_time").alias("ingestion_time")
)
print(f"⚡ Đã Cache thành công {df_exploded.count()} lượt nghe cá nhân vào RAM!")

# COMMAND ----------

# DBTITLE 1,02. Trích xuất Bảng Chiều: dim_albums
# 1. Trích xuất Album từ lịch sử cá nhân
dim_albums_personal = df_exploded.select(
    F.col("item.track.album.id").alias("album_id"),
    F.col("item.track.album.name").alias("album_name"),
    F.col("item.track.album.album_type").alias("album_type"),
    F.col("item.track.album.release_date").alias("release_date"),
    F.col("item.track.album.total_tracks").cast("integer").alias("total_tracks"),
    F.col("item.track.album.images")[0]["url"].alias("image_url"),
    F.col("item.track.album.external_urls.spotify").alias("spotify_url")
).filter("album_id IS NOT NULL")

# 2. Nếu có Album Catalog, trích xuất đầy đủ metadata album
if has_album_catalog and df_albums_raw:
    df_albums_exploded = df_albums_raw.select(F.explode("albums").alias("album"))
    dim_albums_catalog = df_albums_exploded.select(
        F.col("album.id").alias("album_id"),
        F.col("album.name").alias("album_name"),
        F.col("album.album_type").alias("album_type"),
        F.col("album.release_date").alias("release_date"),
        F.col("album.total_tracks").cast("integer").alias("total_tracks"),
        F.col("album.images")[0]["url"].alias("image_url"),
        F.col("album.external_urls.spotify").alias("spotify_url")
    ).filter("album_id IS NOT NULL")

    dim_albums_df = dim_albums_catalog.unionByName(dim_albums_personal).dropDuplicates(["album_id"])
else:
    dim_albums_df = dim_albums_personal.dropDuplicates(["album_id"])

print(f"💿 Tổng số Album trong danh mục Silver: {dim_albums_df.count()}")

# COMMAND ----------

# DBTITLE 1,03. Trích xuất dim_tracks (Gồm cả bài đã nghe & bài chưa nghe trong album)
# 1. Track từ lịch sử nghe cá nhân
tracks_personal = df_exploded.select(
    F.col("item.track.id").alias("track_id"),
    F.col("item.track.name").alias("track_name"),
    F.col("item.track.duration_ms").cast("integer").alias("duration_ms"),
    F.col("item.track.explicit").cast("boolean").alias("explicit"),
    F.col("item.track.album.id").alias("album_id"),
    F.col("item.track.album.name").alias("album_name"),
    F.col("item.track.album.images")[0]["url"].alias("image_url"),
    F.col("item.track.external_urls.spotify").alias("spotify_url"),
    F.col("item.track.track_number").cast("integer").alias("track_number")
).filter("track_id IS NOT NULL")

# 2. Track từ Album Catalog (Toàn bộ tracklist của các Album)
if has_album_catalog and df_albums_raw:
    tracks_catalog = df_albums_exploded.select(
        F.col("album.id").alias("album_id"),
        F.col("album.name").alias("album_name"),
        F.col("album.images")[0]["url"].alias("image_url"),
        F.explode("album.tracks.items").alias("tr")
    ).select(
        F.col("tr.id").alias("track_id"),
        F.col("tr.name").alias("track_name"),
        F.col("tr.duration_ms").cast("integer").alias("duration_ms"),
        F.col("tr.explicit").cast("boolean").alias("explicit"),
        F.col("album_id"),
        F.col("album_name"),
        F.col("image_url"),
        F.col("tr.external_urls.spotify").alias("spotify_url"),
        F.col("tr.track_number").cast("integer").alias("track_number")
    ).filter("track_id IS NOT NULL")

    dim_tracks_df = tracks_catalog.unionByName(tracks_personal).dropDuplicates(["track_id"])
else:
    dim_tracks_df = tracks_personal.dropDuplicates(["track_id"])

print(f"🎵 Tổng số bài hát (Đã nghe + Chưa nghe trong Album): {dim_tracks_df.count()}")

# COMMAND ----------

# DBTITLE 1,04. Trích xuất dim_artists & bridge_track_artists
# 1. Trích xuất nghệ sĩ từ lịch sử nghe
artists_personal = df_exploded.select(
    F.col("item.track.id").alias("track_id"),
    F.col("item.track.album.images")[0]["url"].alias("album_image_url"),
    F.explode("item.track.artists").alias("artist")
)

# 2. Trích xuất nghệ sĩ từ Album Catalog nếu có
if has_album_catalog and df_albums_raw:
    artists_catalog = df_albums_exploded.select(
        F.col("album.images")[0]["url"].alias("album_image_url"),
        F.explode("album.tracks.items").alias("tr")
    ).select(
        F.col("tr.id").alias("track_id"),
        F.col("album_image_url"),
        F.explode("tr.artists").alias("artist")
    )
    all_artists_exploded = artists_personal.unionByName(artists_catalog)
else:
    all_artists_exploded = artists_personal

# Bảng DIM_ARTISTS
dim_artists_df = all_artists_exploded.groupBy(
    F.col("artist.id").alias("artist_id"),
    F.col("artist.name").alias("artist_name"),
    F.col("artist.external_urls.spotify").alias("spotify_url")
).agg(
    F.first("album_image_url", ignorenulls=True).alias("sample_image_url")
).filter("artist_id IS NOT NULL AND artist_name IS NOT NULL")

# Bảng BRIDGE_TRACK_ARTISTS (Cầu nối quan hệ Nhiều - Nhiều)
bridge_track_artists_df = all_artists_exploded.select(
    F.col("track_id"),
    F.col("artist.id").alias("artist_id")
).filter("track_id IS NOT NULL AND artist_id IS NOT NULL").dropDuplicates(["track_id", "artist_id"])

# COMMAND ----------

# DBTITLE 1,05. Trích xuất Bảng FACT_STREAMS (Lượt nghe thực tế)
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

# DBTITLE 1,06. Upsert (MERGE INTO) vào các Bảng Silver

# --- 1. UPSERT VÀO DIM_ALBUMS ---
dim_albums_table = "spotify_silver.dim_albums"
if not spark.catalog.tableExists(dim_albums_table):
    dim_albums_df.write.format("delta").mode("overwrite").saveAsTable(dim_albums_table)
    print("✅ Đã tạo bảng Silver: dim_albums")
else:
    delta_albums = DeltaTable.forName(spark, dim_albums_table)
    delta_albums.alias("target").merge(
        broadcast(dim_albums_df).alias("source"),
        "target.album_id = source.album_id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    print("🔄 Đã MERGE INTO: dim_albums")

# --- 2. UPSERT VÀO DIM_TRACKS ---
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
    print("🔄 Đã MERGE INTO: dim_tracks")

# --- 3. UPSERT VÀO DIM_ARTISTS ---
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
    print("🔄 Đã MERGE INTO: dim_artists")

# --- 4. UPSERT VÀO BRIDGE_TRACK_ARTISTS ---
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
    print("🔄 Đã MERGE INTO: bridge_track_artists")

# --- 5. UPSERT VÀO FACT_STREAMS ---
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
    print("🔄 Đã MERGE INTO: fact_streams")

print("🎉 Hoàn tất xử lý Silver Layer với đầy đủ Album Catalog và Danh sách bài hát chưa nghe!")