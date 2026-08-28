import os
import sys
import glob
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

for env_var in ["REQUESTS_CA_BUNDLE", "CURL_CA_BUNDLE", "SSL_CERT_FILE"]:
    if env_var in os.environ and not os.path.exists(os.environ[env_var]):
        os.environ.pop(env_var, None)

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

spark = SparkSession.builder \
    .appName("Spotify_Silver_Local_Test") \
    .master("local[*]") \
    .getOrCreate()

print("✅ 1. Khởi tạo PySpark Local Session thành công!")

local_pattern = os.path.abspath("data/bronze_spotify_raw/personal_*.json").replace("\\", "/")
json_files = [f.replace("\\", "/") for f in glob.glob(local_pattern)]

if not json_files:
    print("⚠️ Không tìm thấy file JSON thô trong data/bronze_spotify_raw/")
    sys.exit(1)

print(f"📥 2. Đã tìm thấy {len(json_files)} file JSON thô local: {json_files[0]}")
df_raw = spark.read.option("multiline", "true").json(json_files)

df_exploded = df_raw.select(
    F.explode("items").alias("item"),
    F.col("ingestion_metadata.ingestion_time").alias("ingestion_time")
)
print(f"📊 3. Tổng số dòng sau khi explode: {df_exploded.count()}")

df_flattened = df_exploded.select(
    F.to_timestamp(F.col("item.played_at")).alias("played_at"),
    F.col("item.track.id").alias("track_id"),
    F.col("item.track.name").alias("track_name"),
    F.col("item.track.duration_ms").cast("integer").alias("duration_ms"),
    F.col("item.track.explicit").cast("boolean").alias("explicit"),
    F.col("item.track.album.id").alias("album_id"),
    F.col("item.track.album.name").alias("album_name"),
    F.explode("item.track.artists").alias("artist"),
    F.col("ingestion_time")
).select(
    "played_at",
    "track_id",
    "track_name",
    "duration_ms",
    "explicit",
    "album_id",
    "album_name",
    F.col("artist.id").alias("artist_id"),
    F.col("artist.name").alias("artist_name"),
    "ingestion_time"
)

dim_tracks_df = df_flattened.select(
    "track_id", "track_name", "duration_ms", "explicit", "album_id"
).dropDuplicates(["track_id"])
print(f"🎵 4. Bảng dim_tracks có {dim_tracks_df.count()} bài hát độc bản:")
dim_tracks_df.show(5, truncate=False)

dim_artists_df = df_flattened.select("artist_id", "artist_name").dropDuplicates(["artist_id"])
print(f"🎤 5. Bảng dim_artists có {dim_artists_df.count()} nghệ sĩ độc bản:")
dim_artists_df.show(5, truncate=False)

fact_streams_df = df_flattened.select(
    "played_at", "track_id", "artist_id", "album_id", "ingestion_time"
).dropDuplicates(["played_at", "track_id"])
print(f"🎧 6. Bảng fact_streams có {fact_streams_df.count()} lượt nghe độc bản:")
fact_streams_df.show(5, truncate=False)

print("🎉 XỬ LÝ KIỂM THỬ THÀNH CÔNG 100%!")
