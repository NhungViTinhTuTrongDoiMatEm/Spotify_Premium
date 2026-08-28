"""
Module trích xuất dữ liệu Bảng xếp hạng Spotify Hàng ngày (Daily Top 50 Regional & Global Charts).
Chạy định kỳ bởi GitHub Actions hoặc chạy thủ công.
Tự động cào lượt stream thực tế (daily_streams), rank, track_id từ Spotify Charts công khai,
bổ sung thông tin chi tiết qua Spotify API và tải về Databricks Workspace Files.
"""

import os
import sys
import json
import base64
from datetime import datetime, timezone
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Khắc phục lỗi in Unicode/Emoji trên Terminal Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

# Tự động khắc phục sự cố biến chứng chỉ SSL hỏng của Windows (PostgreSQL cũ)
for env_var in ["REQUESTS_CA_BUNDLE", "CURL_CA_BUNDLE", "SSL_CERT_FILE"]:
    if env_var in os.environ and not os.path.exists(os.environ[env_var]):
        os.environ.pop(env_var, None)

load_dotenv()

CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

CHARTS_CONFIG = {
    "VN": {"url": "https://kworb.net/spotify/country/vn_daily.html", "region_name": "Vietnam"},
    "GLOBAL": {"url": "https://kworb.net/spotify/country/global_daily.html", "region_name": "Global"},
    "US": {"url": "https://kworb.net/spotify/country/us_daily.html", "region_name": "United States"},
}


def get_spotify_client_credentials_token():
    """Lấy Client Credentials Access Token để query thông tin track chung."""
    if not CLIENT_ID or not CLIENT_SECRET:
        print("⚠️ Thiếu SPOTIFY_CLIENT_ID hoặc SPOTIFY_CLIENT_SECRET.")
        return None

    url = "https://accounts.spotify.com/api/token"
    payload = {"grant_type": "client_credentials"}
    response = requests.post(url, data=payload, auth=(CLIENT_ID, CLIENT_SECRET))

    if response.status_code == 200:
        return response.json().get("access_token")
    else:
        print(f"⚠️ Không thể lấy Client Credentials Token ({response.status_code}): {response.text}")
        return None


def scrape_spotify_chart(region_code, config):
    """Cào dữ liệu HTML Spotify Daily Chart cho một vùng miền."""
    url = config["url"]
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"❌ Lỗi truy cập bàng xếp hạng {region_code} ({response.status_code}): {url}")
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table", {"id": "spotifydaily"}) or soup.find("table", {"class": "sortable"})
    if not table:
        print(f"⚠️ Không tìm thấy bảng dữ liệu chart cho {region_code}")
        return []

    chart_items = []
    rows = table.find_all("tr")[1:]  # Bỏ qua header

    for row in rows[:50]:  # Lấy Top 50 bài hát
        cols = row.find_all("td")
        if len(cols) < 3:
            continue

        try:
            rank_text = cols[0].text.strip()
            rank = int(rank_text) if rank_text.isdigit() else len(chart_items) + 1

            # Tách tên nghệ sĩ và bài hát từ cột thứ 2
            artist_track_name = cols[1].text.strip()

            # Trích xuất Spotify Track ID từ link href nếu có
            track_id = None
            link = cols[1].find("a")
            if link and "href" in link.attrs:
                href = link["href"]
                if "artist/" in href or "track/" in href:
                    parts = href.split("/")
                    if len(parts) > 1:
                        track_id = parts[-1].replace(".html", "")

            # Trích xuất số lượt streams trong ngày (daily_streams)
            streams_text = cols[2].text.strip().replace(",", "").replace(".", "")
            daily_streams = int(streams_text) if streams_text.isdigit() else 0

            chart_items.append({
                "rank": rank,
                "track_id": track_id,
                "artist_track_name": artist_track_name,
                "daily_streams": daily_streams,
            })
        except Exception as e:
            continue

    print(f"✅ Đã cào thành công Top {len(chart_items)} bài hát cho khu vực {region_code} ({config['region_name']}).")
    return chart_items


def enrich_tracks_with_spotify_api(chart_items, access_token):
    """Bổ sung metadata (album, duration_ms, popularity) cho danh sách track qua Spotify API batch endpoint."""
    if not access_token:
        return chart_items

    # Gom các track_id hợp lệ thành từng lô 50 tracks
    track_ids = [item["track_id"] for item in chart_items if item.get("track_id")]
    if not track_ids:
        return chart_items

    # Gọi Spotify API GET /v1/tracks?ids=...
    url = f"https://api.spotify.com/v1/tracks?ids={','.join(track_ids[:50])}"
    headers = {"Authorization": f"Bearer {access_token}"}
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        tracks_data = response.json().get("tracks", [])
        tracks_map = {t["id"]: t for t in tracks_data if t}

        for item in chart_items:
            t_id = item.get("track_id")
            if t_id and t_id in tracks_map:
                t_info = tracks_map[t_id]
                item["track_name"] = t_info.get("name")
                item["duration_ms"] = t_info.get("duration_ms")
                item["popularity"] = t_info.get("popularity")
                item["explicit"] = t_info.get("explicit")
                item["album_id"] = t_info.get("album", {}).get("id")
                item["album_name"] = t_info.get("album", {}).get("name")
                item["artists"] = [
                    {"artist_id": a.get("id"), "artist_name": a.get("name")}
                    for a in t_info.get("artists", [])
                ]

    return chart_items


def upload_to_databricks_workspace(data_json, target_path):
    """Lưu file JSON thô ở local VÀ đẩy lên Databricks Workspace Files."""
    # 1. Luôn luôn lưu 1 bản sao file JSON thô ở máy local
    os.makedirs("data/bronze_spotify_daily_charts_raw", exist_ok=True)
    local_filename = os.path.join("data/bronze_spotify_daily_charts_raw", os.path.basename(target_path))
    with open(local_filename, "w", encoding="utf-8") as f:
        json.dump(data_json, f, ensure_ascii=False, indent=2)
    print(f"💾 Đã lưu dữ liệu Chart local tại: {local_filename}")

    # 2. Kiểm tra cấu hình Databricks
    if not DATABRICKS_HOST or not DATABRICKS_TOKEN or "your_databricks" in DATABRICKS_TOKEN:
        print("ℹ️ Chưa cấu hình Databricks Host/Token. Bỏ qua bước đẩy lên Databricks Workspace.")
        return

    host = DATABRICKS_HOST.rstrip("/")
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json",
    }

    # 3. Tự động tạo thư mục cha trên Databricks Workspace
    parent_dir = os.path.dirname(target_path).replace("\\", "/")
    if parent_dir:
        mkdirs_url = f"{host}/api/2.0/workspace/mkdirs"
        requests.post(mkdirs_url, headers=headers, json={"path": parent_dir})

    # 4. Đẩy file JSON thô vào Databricks Workspace qua REST API
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
        print(f"🚀 [SUCCESS] Đã tải thành công dữ liệu Daily Chart lên Databricks Workspace: {target_path}")
    else:
        print(f"❌ Lỗi ghi file Chart vào Databricks Workspace ({response.status_code}): {response.text}")


def main():
    print("🌐 [1/3] Đang cào dữ liệu Bảng xếp hạng Spotify Daily Top 50 (VN, Global, US)...")
    access_token = get_spotify_client_credentials_token()

    now_utc = datetime.now(timezone.utc)
    chart_date = now_utc.strftime("%Y-%m-%d")
    batch_id = now_utc.strftime("%Y%m%d_%H%M%S")

    charts_payload = {
        "ingestion_metadata": {
            "ingestion_time": now_utc.isoformat(),
            "batch_id": batch_id,
            "chart_date": chart_date,
        },
        "regions": {},
    }

    for region_code, config in CHARTS_CONFIG.items():
        items = scrape_spotify_chart(region_code, config)
        if access_token and items:
            items = enrich_tracks_with_spotify_api(items, access_token)

        charts_payload["regions"][region_code] = {
            "region_name": config["region_name"],
            "item_count": len(items),
            "items": items,
        }

    target_path = f"/Workspace/spotify_charts_raw/charts_{batch_id}.json"

    print("📤 [3/3] Đang đẩy dữ liệu Bảng xếp hạng Bronze vào Databricks Workspace...")
    upload_to_databricks_workspace(charts_payload, target_path)


if __name__ == "__main__":
    main()
