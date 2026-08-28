"""
Module trích xuất dữ liệu nghe nhạc cá nhân (Recently Played Tracks).
Thực thi định kỳ bởi GitHub Actions hoặc chạy thủ công.
Gửi payload JSON thô vào Databricks Workspace Files: /Workspace/spotify_raw/
"""

import os
import sys
import json
import base64
from datetime import datetime, timezone
import requests
from dotenv import load_dotenv

# Khắc phục lỗi in Unicode/Emoji trên Terminal Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

# Tự động khắc phục sự cố biến chứng chỉ SSL hỏng của Windows (PostgreSQL cũ)
for env_var in ["REQUESTS_CA_BUNDLE", "CURL_CA_BUNDLE", "SSL_CERT_FILE"]:
    if env_var in os.environ and not os.path.exists(os.environ[env_var]):
        os.environ.pop(env_var, None)

load_dotenv()

# Lấy thông tin cấu hình từ biến môi trường
CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("SPOTIFY_REFRESH_TOKEN")

DATABRICKS_HOST = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")


def get_spotify_access_token():
    """Tự động đổi Refresh Token lấy Access Token mới có hiệu lực 1 tiếng."""
    if not CLIENT_ID or not CLIENT_SECRET or not REFRESH_TOKEN:
        print("❌ Lỗi: Thiếu SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET hoặc SPOTIFY_REFRESH_TOKEN trong môi trường/file .env!")
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


def fetch_recently_played(access_token, limit=50):
    """Gọi Spotify API để lấy danh sách các bài hát nghe gần nhất (tối đa 50 bài)."""
    url = f"https://api.spotify.com/v1/me/player/recently-played?limit={limit}"
    headers = {"Authorization": f"Bearer {access_token}"}

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"❌ Lỗi gọi Spotify Recently Played API ({response.status_code}): {response.text}")
        sys.exit(1)


def upload_to_databricks_workspace(data_json, target_path):
    """Lưu file JSON thô ở local VÀ đẩy lên Databricks Workspace Files (Chuẩn Databricks mới)."""
    # 1. Luôn luôn lưu 1 bản sao file JSON thô ở máy local
    os.makedirs("data/bronze_spotify_raw", exist_ok=True)
    local_filename = os.path.join("data/bronze_spotify_raw", os.path.basename(target_path))
    with open(local_filename, "w", encoding="utf-8") as f:
        json.dump(data_json, f, ensure_ascii=False, indent=2)
    print(f"💾 Đã lưu dữ liệu Bronze local tại: {local_filename}")

    # 2. Kiểm tra cấu hình Databricks
    if not DATABRICKS_HOST or not DATABRICKS_TOKEN or "your_databricks" in DATABRICKS_TOKEN:
        print("ℹ️ Chưa cấu hình DATABRICKS_HOST/DATABRICKS_TOKEN. Bỏ qua bước đẩy lên Databricks Workspace.")
        return

    host = DATABRICKS_HOST.strip().rstrip("/")
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json",
    }

    # 3. Tự động tạo thư mục cha trên Databricks Workspace nếu chưa tồn tại
    parent_dir = os.path.dirname(target_path).replace("\\", "/")
    if parent_dir:
        mkdirs_url = f"{host}/api/2.0/workspace/mkdirs"
        requests.post(mkdirs_url, headers=headers, json={"path": parent_dir})

    # 4. Đẩy file JSON thô vào Databricks Workspace bằng REST API
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
        print(f"🚀 [SUCCESS] Đã tải thành công dữ liệu Bronze lên Databricks Workspace: {target_path}")
    else:
        print(f"❌ Lỗi ghi file vào Databricks Workspace ({response.status_code}): {response.text}")


def main():
    print("🔄 [1/3] Đang khởi tạo Access Token từ Spotify Refresh Token...")
    access_token = get_spotify_access_token()

    print("📥 [2/3] Đang trích xuất 50 lịch sử nghe gần nhất từ Spotify API...")
    payload = fetch_recently_played(access_token, limit=50)

    # Thêm Metadata quản lý dữ liệu Bronze
    now_utc = datetime.now(timezone.utc)
    ingestion_timestamp = now_utc.isoformat()
    batch_id = now_utc.strftime("%Y%m%d_%H%M%S")

    payload["ingestion_metadata"] = {
        "ingestion_time": ingestion_timestamp,
        "batch_id": batch_id,
        "record_count": len(payload.get("items", [])),
    }

    print(f"📊 Thu thập thành công {len(payload.get('items', []))} bản ghi nghe nhạc.")

    # Đặt tên file dữ liệu thô trên Databricks Workspace Files
    target_path = f"/Workspace/spotify_raw/personal_{batch_id}.json"

    print("📤 [3/3] Đang đẩy dữ liệu thô Bronze vào Databricks Workspace...")
    upload_to_databricks_workspace(payload, target_path)


if __name__ == "__main__":
    main()