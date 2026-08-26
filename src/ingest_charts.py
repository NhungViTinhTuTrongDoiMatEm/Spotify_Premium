"""
Module trích xuất dữ liệu Bảng xếp hạng Hàng ngày (Daily Top 50 Regional & Global Charts).
Thực thi định kỳ bởi GitHub Actions (1 lần/ngày lúc 13:00 UTC) hoặc chạy thủ công.
Nguồn dữ liệu: Daily Spotify Charts (VN, GLOBAL, US) đính kèm lượt stream và thứ hạng.
Gửi payload JSON thô vào Databricks DBFS: dbfs:/FileStore/spotify/bronze_charts/
"""

import os
import sys
import json
import base64
from datetime import datetime, timezone
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("SPOTIFY_REFRESH_TOKEN")

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

# Danh sách URL Daily Charts theo Quốc gia
CHART_SOURCES = {
    "VN": {"url": "https://kworb.net/spotify/country/vn_daily.html", "name": "Daily Top 50 Vietnam"},
    "GLOBAL": {"url": "https://kworb.net/spotify/country/global_daily.html", "name": "Daily Top 50 Global"},
    "US": {"url": "https://kworb.net/spotify/country/us_daily.html", "name": "Daily Top 50 USA"},
}


def get_spotify_access_token():
    """Tự động đổi Refresh Token lấy Access Token mới."""
    if not CLIENT_ID or not CLIENT_SECRET or not REFRESH_TOKEN:
        print("❌ Lỗi: Thiếu SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET hoặc SPOTIFY_REFRESH_TOKEN!")
        sys.exit(1)

    url = "https://accounts.spotify.com/api/token"
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": REFRESH_TOKEN,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }

    response = requests.post(url, data=payload)
    if response.status_code == 200:
        return response.json()["access_token"]
    else:
        print(f"❌ Lỗi cấp lại Access Token ({response.status_code}): {response.text}")
        sys.exit(1)


def fetch_daily_chart_data(chart_url, limit=50):
    """Cào bảng xếp hạng Daily Top 50 cùng lượt stream trong ngày."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    response = requests.get(chart_url, headers=headers)
    if response.status_code != 200:
        print(f"❌ Không thể tải dữ liệu từ {chart_url} (HTTP {response.status_code})")
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table", {"id": "spotifydaily"}) or soup.find("table")
    if not table:
        return []

    tracks = []
    rows = table.find_all("tr")[1:limit+1]  # Lấy top 50 bài hát

    for row in rows:
        tds = row.find_all("td")
        if len(tds) >= 7:
            rank_str = tds[0].get_text(strip=True)
            artist_track_raw = tds[2].get_text(strip=True)
            daily_streams_raw = tds[6].get_text(strip=True).replace(",", "")
            
            # Trích xuất Spotify Track ID từ thẻ liên kết <a> nếu có
            link_tag = tds[2].find("a")
            track_id = None
            if link_tag and "href" in link_tag.attrs:
                href = link_tag["href"]
                if "/track/" in href:
                    track_id = href.split("/track/")[-1].replace(".html", "").split("?")[0]

            try:
                rank = int(rank_str)
            except ValueError:
                rank = len(tracks) + 1

            try:
                daily_streams = int(daily_streams_raw)
            except ValueError:
                daily_streams = 0

            tracks.append({
                "rank": rank,
                "artist_track_name": artist_track_raw,
                "track_id": track_id,
                "daily_streams": daily_streams,
            })

    return tracks


def enrich_tracks_with_spotify_api(access_token, tracks):
    """Bổ sung chi tiết nghệ sĩ, album, popularity từ Spotify API cho bài hát."""
    headers = {"Authorization": f"Bearer {access_token}"}
    enriched = []

    for item in tracks:
        track_id = item.get("track_id")
        track_detail = {
            "rank": item["rank"],
            "track_id": track_id,
            "artist_track_name": item["artist_track_name"],
            "daily_streams": item["daily_streams"],
            "track_name": None,
            "artist_name": None,
            "popularity": None,
            "album": None,
        }

        # Nếu tìm thấy track_id, gọi Spotify API lấy thông tin chi tiết
        if track_id:
            api_url = f"https://api.spotify.com/v1/tracks/{track_id}"
            res = requests.get(api_url, headers=headers)
            if res.status_code == 200:
                t = res.json()
                track_detail["track_name"] = t.get("name")
                track_detail["popularity"] = t.get("popularity")
                track_detail["duration_ms"] = t.get("duration_ms")
                track_detail["explicit"] = t.get("explicit")
                track_detail["album"] = {
                    "album_id": t.get("album", {}).get("id"),
                    "album_name": t.get("album", {}).get("name"),
                    "release_date": t.get("album", {}).get("release_date"),
                }
                track_detail["artists"] = [
                    {"artist_id": a.get("id"), "artist_name": a.get("name")}
                    for a in t.get("artists", [])
                ]

        enriched.append(track_detail)

    return enriched


def upload_to_databricks_dbfs(data_json, dbfs_path):
    """Đẩy payload JSON lên Databricks DBFS thông qua REST API."""
    if not DATABRICKS_HOST or not DATABRICKS_TOKEN or "your_databricks" in DATABRICKS_TOKEN:
        print("ℹ️ Chưa cấu hình Databricks Host/Token. Đang lưu tạm file JSON local...")
        os.makedirs("data/bronze_spotify_daily_charts_raw", exist_ok=True)
        local_filename = os.path.join("data/bronze_spotify_daily_charts_raw", os.path.basename(dbfs_path))
        with open(local_filename, "w", encoding="utf-8") as f:
            json.dump(data_json, f, ensure_ascii=False, indent=2)
        print(f"💾 Đã lưu dữ liệu Chart local tại: {local_filename}")
        return

    host = DATABRICKS_HOST.rstrip("/")
    api_url = f"{host}/api/2.0/dbfs/put"

    json_bytes = json.dumps(data_json, ensure_ascii=False).encode("utf-8")
    content_b64 = base64.b64encode(json_bytes).decode("utf-8")

    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {
        "path": dbfs_path,
        "contents": content_b64,
        "overwrite": True,
    }

    response = requests.post(api_url, headers=headers, json=payload)
    if response.status_code == 200:
        print(f"🚀 [SUCCESS] Đã đẩy dữ liệu Chart lên Databricks DBFS: {dbfs_path}")
    else:
        print(f"❌ Lỗi ghi file vào Databricks DBFS ({response.status_code}): {response.text}")


def main():
    print("🔄 [1/3] Đang lấy Access Token mới từ Spotify...")
    access_token = get_spotify_access_token()

    now_utc = datetime.now(timezone.utc)
    snapshot_date = now_utc.strftime("%Y-%m-%d")
    batch_id = now_utc.strftime("%Y%m%d_%H%M%S")

    print(f"📥 [2/3] Bắt đầu cào dữ liệu Bảng xếp hạng Daily Top 50 (Ngày: {snapshot_date})...")

    for country_code, chart_info in CHART_SOURCES.items():
        print(f"  --> Đang cào bảng xếp hạng: {chart_info['name']} ({country_code})...")
        raw_tracks = fetch_daily_chart_data(chart_info["url"], limit=50)

        if not raw_tracks:
            print(f"      ⚠️ Không lấy được dữ liệu cho {country_code}.")
            continue

        print(f"      🔄 Đang bổ sung metadata từ Spotify API cho {len(raw_tracks)} bài...")
        enriched_tracks = enrich_tracks_with_spotify_api(access_token, raw_tracks[:10])  # Enrich top tracks

        chart_payload = {
            "snapshot_date": snapshot_date,
            "country_code": country_code,
            "chart_name": chart_info["name"],
            "ingestion_time": now_utc.isoformat(),
            "batch_id": batch_id,
            "record_count": len(enriched_tracks),
            "tracks": enriched_tracks,
        }

        print(f"      ✅ Thu thập thành công Top 50 bài hát ({country_code}).")

        # Đẩy file dữ liệu thô Chart lên DBFS
        dbfs_target_path = f"/FileStore/spotify/bronze_charts/charts_{country_code}_{batch_id}.json"
        upload_to_databricks_dbfs(chart_payload, dbfs_target_path)

    print("🎉 Hoàn tất cào dữ liệu Bảng xếp hạng Hàng ngày!")


if __name__ == "__main__":
    main()
