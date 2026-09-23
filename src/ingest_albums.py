"""
Module cào toàn bộ danh sách bài hát (Tracklist) của các Album chứa những bài hát bạn từng nghe.
Sử dụng Spotify API Batch Endpoint: GET /v1/albums?ids=... (20 album/lần gọi)
Lưu kết quả thô vào data/bronze_spotify_raw/ và đồng bộ lên Databricks /Workspace/spotify_raw/
"""

import os
import sys
import glob
import json
import base64
import time
from datetime import datetime, timezone
import requests
from dotenv import load_dotenv

# Cấu hình UTF-8 cho Windows console
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

load_dotenv()

CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("SPOTIFY_REFRESH_TOKEN")

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")


def get_spotify_access_token():
    """Lấy Client Credentials Access Token để truy vấn catalog album công khai mà không bị 403."""
    if not CLIENT_ID or not CLIENT_SECRET:
        print("❌ Lỗi: Thiếu SPOTIFY_CLIENT_ID hoặc SPOTIFY_CLIENT_SECRET trong .env!")
        sys.exit(1)

    url = "https://accounts.spotify.com/api/token"
    payload = {"grant_type": "client_credentials"}

    response = requests.post(url, data=payload, auth=(CLIENT_ID, CLIENT_SECRET), timeout=10)
    if response.status_code != 200:
        print(f"❌ Lỗi lấy token ({response.status_code}): {response.text}")
        sys.exit(1)

    return response.json().get("access_token")


def get_album_ids_to_scrape(max_albums=60):
    """Tìm danh sách các album ID chứa bài hát bạn đã nghe (ưu tiên album có > 1 bài)."""
    album_ids = set()

    # 1. Quét từ các file JSON Bronze local
    bronze_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "bronze_spotify_raw")
    json_files = glob.glob(os.path.join(bronze_dir, "personal_*.json"))

    for file_path in json_files:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                for item in data.get("items", []):
                    album = item.get("track", {}).get("album", {})
                    a_id = album.get("id")
                    total_tracks = album.get("total_tracks", 1)
                    if a_id and total_tracks > 1:
                        album_ids.add(a_id)
        except Exception:
            continue

    # 2. Bổ sung từ Databricks Lakehouse nếu kết nối được
    try:
        from src.databricks_client import DatabricksLakehouseClient
        client = DatabricksLakehouseClient()
        if client.is_configured():
            q = """
            SELECT DISTINCT item.track.album.id AS album_id
            FROM (SELECT explode(items) AS item FROM read_files('/Workspace/spotify_raw/personal_*.json', format => 'json'))
            WHERE item.track.album.total_tracks > 1 AND item.track.album.id IS NOT NULL
            """
            rows = client.execute_sql(q)
            if rows:
                for r in rows:
                    if r.get("album_id"):
                        album_ids.add(r["album_id"])
    except Exception:
        pass

    album_list = list(album_ids)
    print(f"🔍 Tìm thấy {len(album_list)} Album chứa nhiều bài hát cần cào tracklist.")
    return album_list[:max_albums]


def fetch_albums(album_ids, access_token):
    """Cào chi tiết album và toàn bộ tracklist của từng album (tránh 403 của batch endpoint)."""
    headers = {"Authorization": f"Bearer {access_token}"}
    all_albums = []

    print(f"📥 Đang cào tracklist cho {len(album_ids)} Album qua Spotify API...")
    for idx, a_id in enumerate(album_ids, 1):
        url = f"https://api.spotify.com/v1/albums/{a_id}"
        try:
            res = requests.get(url, headers=headers, timeout=10)
            if res.status_code == 200:
                data = res.json()
                tracks_count = len(data.get("tracks", {}).get("items", []))
                album_name = data.get("name", "Unknown Album")
                all_albums.append(data)
                print(f"  [{idx}/{len(album_ids)}] ✅ {album_name} ({tracks_count} bài)")
            elif res.status_code == 429:
                retry_after = int(res.headers.get("Retry-After", 2))
                time.sleep(retry_after)
            else:
                print(f"  [{idx}/{len(album_ids)}] ⚠️ Lỗi Album {a_id} ({res.status_code})")
            time.sleep(0.06)
        except Exception as e:
            print(f"  [{idx}/{len(album_ids)}] ❌ Ngoại lệ {a_id}: {e}")

    return all_albums


def upload_to_databricks_workspace(data_json, target_path):
    """Tải dữ liệu Album thô lên Databricks Workspace Files."""
    if not DATABRICKS_HOST or not DATABRICKS_TOKEN or "your_databricks" in DATABRICKS_TOKEN:
        print("ℹ️ Chưa cấu hình Databricks token. Bỏ qua tải lên workspace.")
        return

    host = DATABRICKS_HOST.strip().rstrip("/")
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json",
    }

    parent_dir = os.path.dirname(target_path).replace("\\", "/")
    if parent_dir:
        requests.post(f"{host}/api/2.0/workspace/mkdirs", headers=headers, json={"path": parent_dir})

    api_url = f"{host}/api/2.0/workspace/import"
    json_bytes = json.dumps(data_json, ensure_ascii=False).encode("utf-8")
    content_b64 = base64.b64encode(json_bytes).decode("utf-8")

    payload = {
        "path": target_path,
        "content": content_b64,
        "format": "AUTO",
        "overwrite": True,
    }

    response = requests.post(api_url, headers=headers, json=payload)
    if response.status_code == 200:
        print(f"🚀 [SUCCESS] Đã tải thành công Album Catalog lên Databricks: {target_path}")
    else:
        print(f"❌ Lỗi ghi file Album vào Databricks ({response.status_code}): {response.text}")


def main():
    print("🚀 Bắt đầu quy trình cào toàn bộ Album & Tracklist chứa bài hát...")
    token = get_spotify_access_token()

    album_ids = get_album_ids_to_scrape(max_albums=100)
    if not album_ids:
        print("ℹ️ Không tìm thấy album nào cần cào.")
        return

    albums = fetch_albums(album_ids, token)
    print(f"✅ Đã cào thành công {len(albums)} album với đầy đủ tracklist!")

    now_utc = datetime.now(timezone.utc)
    batch_id = now_utc.strftime("%Y%m%d_%H%M%S")

    payload = {
        "ingestion_metadata": {
            "ingestion_time": now_utc.isoformat(),
            "batch_id": batch_id,
            "album_count": len(albums),
            "total_tracks_count": sum(len(a.get("tracks", {}).get("items", [])) for a in albums)
        },
        "albums": albums
    }

    # Lưu cục bộ
    bronze_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "bronze_spotify_raw")
    os.makedirs(bronze_dir, exist_ok=True)
    local_file = os.path.join(bronze_dir, f"album_catalog_{batch_id}.json")
    with open(local_file, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    print(f"💾 Đã lưu cục bộ: {local_file}")

    # Đẩy lên Databricks
    target_path = f"/Workspace/spotify_raw/album_catalog_{batch_id}.json"
    upload_to_databricks_workspace(payload, target_path)


if __name__ == "__main__":
    main()
