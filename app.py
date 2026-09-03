"""
Spotify Premium Analytics & Music Explorer - Backend Server
Đọc và phân tích dữ liệu Bronze từ data/bronze_spotify_raw/
Cung cấp REST API và phục vụ giao diện Web trực quan chuẩn Spotify.
"""

import os
import sys
import glob
import json
import subprocess
from datetime import datetime, timezone
from http.server import SimpleHTTPRequestHandler, HTTPServer
import urllib.parse

# Cấu hình UTF-8 cho Windows
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

PORT = 5000
DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "bronze_spotify_raw")
WEB_DIR = os.path.join(os.path.dirname(__file__), "web")


def get_time_slot(hour):
    """Xác định khung giờ nghe nhạc."""
    if 5 <= hour < 12:
        return "Sáng (05h-12h)"
    elif 12 <= hour < 18:
        return "Chiều (12h-18h)"
    elif 18 <= hour < 23:
        return "Tối (18h-23h)"
    else:
        return "Đêm (23h-05h)"


def load_and_aggregate_data():
    """Đọc toàn bộ file JSON thô và thực hiện logic tổng hợp tương đương tầng Silver/Gold."""
    json_files = glob.glob(os.path.join(DATA_DIR, "personal_*.json"))
    if not json_files:
        return {
            "total_streams": 0,
            "unique_tracks": 0,
            "unique_artists": 0,
            "total_hours": 0.0,
            "diversity_ratio": 0.0,
            "top_tracks": [],
            "top_artists": [],
            "all_tracks": [],
            "all_artists": [],
            "time_schedule": {},
            "hourly_distribution": [0] * 24,
            "latest_sync": None
        }

    # Bảng trung gian
    streams = []
    tracks_dict = {}
    artists_dict = {}
    seen_streams = set()

    for file_path in json_files:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                payload = json.load(f)
                items = payload.get("items", [])
                for item in items:
                    track_data = item.get("track", {})
                    if not track_data or not track_data.get("id"):
                        continue

                    track_id = track_data["id"]
                    played_at_str = item.get("played_at")
                    if not played_at_str:
                        continue

                    # Khử trùng lặp độ mịn (played_at + track_id)
                    stream_key = (played_at_str, track_id)
                    if stream_key in seen_streams:
                        continue
                    seen_streams.add(stream_key)

                    # Parse thời gian
                    try:
                        dt = datetime.fromisoformat(played_at_str.replace("Z", "+00:00"))
                    except Exception:
                        continue

                    # Lấy thông tin Album & Ảnh bìa
                    album = track_data.get("album", {})
                    album_images = album.get("images", [])
                    image_url = album_images[0]["url"] if album_images else ""

                    # Lấy danh sách ca sĩ
                    artist_list = track_data.get("artists", [])
                    artist_names = [a.get("name") for a in artist_list if a.get("name")]
                    artist_names_str = ", ".join(artist_names) if artist_names else "Unknown Artist"

                    duration_ms = track_data.get("duration_ms", 0)
                    spotify_url = track_data.get("external_urls", {}).get("spotify", "")

                    hour = dt.hour
                    time_slot = get_time_slot(hour)

                    streams.append({
                        "played_at": played_at_str,
                        "track_id": track_id,
                        "track_name": track_data.get("name", "Unknown"),
                        "duration_ms": duration_ms,
                        "image_url": image_url,
                        "artist_names": artist_names_str,
                        "album_name": album.get("name", "Unknown Album"),
                        "spotify_url": spotify_url,
                        "time_slot": time_slot,
                        "hour": hour
                    })

                    # Cập nhật thông tin Track
                    if track_id not in tracks_dict:
                        tracks_dict[track_id] = {
                            "track_id": track_id,
                            "track_name": track_data.get("name", "Unknown"),
                            "artist_names": artist_names_str,
                            "album_name": album.get("name", "Unknown Album"),
                            "image_url": image_url,
                            "spotify_url": spotify_url,
                            "duration_ms": duration_ms,
                            "total_streams": 0,
                            "first_listened_at": played_at_str,
                            "last_listened_at": played_at_str,
                        }
                    t_item = tracks_dict[track_id]
                    t_item["total_streams"] += 1
                    if played_at_str < t_item["first_listened_at"]:
                        t_item["first_listened_at"] = played_at_str
                    if played_at_str > t_item["last_listened_at"]:
                        t_item["last_listened_at"] = played_at_str

                    # Cập nhật thông tin Artist (Cả ca sĩ chính và ca sĩ feat qua Bridge)
                    for a in artist_list:
                        a_id = a.get("id")
                        a_name = a.get("name")
                        if not a_id or not a_name:
                            continue
                        if a_id not in artists_dict:
                            artists_dict[a_id] = {
                                "artist_id": a_id,
                                "artist_name": a_name,
                                "spotify_url": a.get("external_urls", {}).get("spotify", ""),
                                "total_streams": 0,
                                "total_duration_ms": 0,
                                "first_listened_at": played_at_str,
                                "last_listened_at": played_at_str,
                                "sample_image": image_url
                            }
                        art_item = artists_dict[a_id]
                        art_item["total_streams"] += 1
                        art_item["total_duration_ms"] += duration_ms
                        if not art_item["sample_image"] and image_url:
                            art_item["sample_image"] = image_url
                        if played_at_str < art_item["first_listened_at"]:
                            art_item["first_listened_at"] = played_at_str
                        if played_at_str > art_item["last_listened_at"]:
                            art_item["last_listened_at"] = played_at_str

        except Exception as e:
            print(f"⚠️ Lỗi đọc file {file_path}: {e}")

    # Tính toán chỉ số tổng hợp
    total_streams_count = len(streams)
    unique_tracks_count = len(tracks_dict)
    unique_artists_count = len(artists_dict)

    total_duration_ms = sum(s["duration_ms"] for s in streams)
    total_hours = round(total_duration_ms / 3600000.0, 2)
    total_minutes = round(total_duration_ms / 60000.0, 1)

    diversity_ratio = round((unique_artists_count / total_streams_count) if total_streams_count > 0 else 0, 3)

    # Thống kê phân bố giờ
    hourly_distribution = [0] * 24
    time_schedule = {
        "Sáng (05h-12h)": 0,
        "Chiều (12h-18h)": 0,
        "Tối (18h-23h)": 0,
        "Đêm (23h-05h)": 0
    }
    for s in streams:
        hourly_distribution[s["hour"]] += 1
        time_schedule[s["time_slot"]] = time_schedule.get(s["time_slot"], 0) + 1

    # Sắp xếp Top bài hát và Top ca sĩ
    all_tracks_sorted = sorted(tracks_dict.values(), key=lambda x: x["total_streams"], reverse=True)
    all_artists_sorted = sorted(artists_dict.values(), key=lambda x: x["total_streams"], reverse=True)

    # Thêm phút nghe cho tracks
    for t in all_tracks_sorted:
        t["total_minutes"] = round((t["duration_ms"] * t["total_streams"]) / 60000.0, 1)

    for a in all_artists_sorted:
        a["total_minutes"] = round(a["total_duration_ms"] / 60000.0, 1)

    top_tracks = all_tracks_sorted[:50]
    top_artists = all_artists_sorted[:50]

    # Nhận diện Persona nghe nhạc
    peak_slot = max(time_schedule.items(), key=lambda x: x[1])[0] if time_schedule else "Cân bằng"
    persona = "Cú Đêm Yêu Âm Nhạc 🦉" if "Đêm" in peak_slot else ("Người Truyền Năng Lượng Sáng ☀️" if "Sáng" in peak_slot else ("Lofi Buổi Chiều 🌇" if "Chiều" in peak_slot else "Âm Nhạc Buổi Tối 🌙"))

    # Lấy thời gian đồng bộ gần nhất
    latest_sync = max((s["played_at"] for s in streams), default=None)

    return {
        "total_streams": total_streams_count,
        "unique_tracks": unique_tracks_count,
        "unique_artists": unique_artists_count,
        "total_hours": total_hours,
        "total_minutes": total_minutes,
        "diversity_ratio": diversity_ratio,
        "persona": persona,
        "peak_slot": peak_slot,
        "time_schedule": time_schedule,
        "hourly_distribution": hourly_distribution,
        "top_tracks": top_tracks,
        "top_artists": top_artists,
        "all_tracks": all_tracks_sorted,
        "all_artists": all_artists_sorted,
        "recent_streams": sorted(streams, key=lambda x: x["played_at"], reverse=True)[:25],
        "latest_sync": latest_sync
    }


class SpotifyAppHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=WEB_DIR, **kwargs)

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path

        if path == "/api/stats":
            data = load_and_aggregate_data()
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps(data, ensure_ascii=False).encode("utf-8"))
            return

        elif path == "/api/search":
            query_params = urllib.parse.parse_qs(parsed.query)
            q = query_params.get("q", [""])[0].strip().lower()
            data = load_and_aggregate_data()

            if not q:
                results = {
                    "matched_tracks": data["all_tracks"][:20],
                    "matched_artists": data["all_artists"][:20]
                }
            else:
                matched_tracks = [
                    t for t in data["all_tracks"]
                    if q in t["track_name"].lower() or q in t["artist_names"].lower() or q in t["album_name"].lower()
                ][:30]
                matched_artists = [
                    a for a in data["all_artists"]
                    if q in a["artist_name"].lower()
                ][:30]
                results = {
                    "query": q,
                    "matched_tracks": matched_tracks,
                    "matched_artists": matched_artists
                }

            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps(results, ensure_ascii=False).encode("utf-8"))
            return

        # Phục vụ file tĩnh từ thư mục web/
        return super().do_GET()

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/api/sync":
            # Kích hoạt pipeline cào dữ liệu mới từ Spotify
            try:
                print("🔄 [API] Kích hoạt cào dữ liệu mới từ Spotify API...")
                python_exe = sys.executable
                script_path = os.path.join(os.path.dirname(__file__), "src", "ingest_personal.py")
                res = subprocess.run([python_exe, script_path], capture_output=True, text=True, timeout=60)
                
                success = res.returncode == 0
                response_data = {
                    "success": success,
                    "output": res.stdout if success else res.stderr
                }
            except Exception as e:
                response_data = {"success": False, "error": str(e)}

            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps(response_data, ensure_ascii=False).encode("utf-8"))
            return

        self.send_response(404)
        self.end_headers()


def run_server():
    os.makedirs(WEB_DIR, exist_ok=True)
    server_address = ("", PORT)
    httpd = HTTPServer(server_address, SpotifyAppHandler)
    print(f"\n=======================================================")
    print(f"🎵 SPOTIFY PREMIUM ANALYTICS & MUSIC EXPLORER WEB APP")
    print(f"🚀 Ứng dụng đang chạy tại: http://localhost:{PORT}")
    print(f"✨ Mở trình duyệt và trải nghiệm ngay!")
    print(f"=======================================================\n")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n🛑 Đã dừng máy chủ.")
        httpd.server_close()


if __name__ == "__main__":
    run_server()
