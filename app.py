"""
Spotify Premium Analytics & Music Explorer - Backend Server
Kết nối trực tiếp với Databricks Lakehouse (Serverless SQL Warehouse) qua REST Execution API.
Tự động fallback sang dữ liệu Bronze JSON local khi offline.
Cung cấp REST API và phục vụ giao diện Web trực quan chuẩn Spotify.
"""

import os
import sys
import glob
import json
import time
import subprocess
from datetime import datetime, timezone
from http.server import SimpleHTTPRequestHandler, HTTPServer
import urllib.parse

# Cấu hình UTF-8 cho Windows console
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

from src.databricks_client import DatabricksLakehouseClient

PORT = 5000
DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "bronze_spotify_raw")
WEB_DIR = os.path.join(os.path.dirname(__file__), "web")

# Bộ nhớ đệm dữ liệu (Cache trong RAM) để web phản hồi siêu tốc (< 5ms)
CACHE_STATE = {
    "data": None,
    "cached_at": 0,
    "ttl_seconds": 300  # 5 phút tự động làm mới từ Lakehouse
}


def get_time_slot(hour: int) -> str:
    """Xác định khung giờ nghe nhạc."""
    if 5 <= hour < 12:
        return "Sáng (05h-12h)"
    elif 12 <= hour < 18:
        return "Chiều (12h-18h)"
    elif 18 <= hour < 23:
        return "Tối (18h-23h)"
    else:
        return "Đêm (23h-05h)"


def load_local_metadata_lookup():
    """Quét các file bronze JSON để tạo bảng tra cứu hình ảnh bìa và link Spotify cho Tracks & Artists."""
    track_meta = {}
    artist_meta = {}
    json_files = glob.glob(os.path.join(DATA_DIR, "personal_*.json"))

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
                    album = track_data.get("album", {})
                    album_images = album.get("images", [])
                    image_url = album_images[0]["url"] if album_images else ""
                    album_name = album.get("name", "Unknown Album")
                    spotify_url = track_data.get("external_urls", {}).get("spotify", "")

                    if track_id not in track_meta:
                        track_meta[track_id] = {
                            "image_url": image_url,
                            "album_name": album_name,
                            "spotify_url": spotify_url,
                            "duration_ms": track_data.get("duration_ms", 0)
                        }

                    for a in track_data.get("artists", []):
                        a_id = a.get("id")
                        if a_id and a_id not in artist_meta:
                            artist_meta[a_id] = {
                                "sample_image": image_url,
                                "spotify_url": a.get("external_urls", {}).get("spotify", "")
                            }
        except Exception:
            continue

    return track_meta, artist_meta


def load_from_databricks_lakehouse(client: DatabricksLakehouseClient):
    """Truy vấn trực tiếp các bảng và Reporting Views tầng Gold trên Databricks Lakehouse."""
    try:
        print("⚡ [Databricks Lakehouse] Đang truy vấn dữ liệu thời gian thực từ Serverless SQL Warehouse...")
        
        # 1. Truy vấn KPI tổng hợp từ view gold_listening_summary
        summary_row = client.fetch_listening_summary()
        if not summary_row:
            print("⚠️ Không lấy được summary từ Databricks, chuyển sang fallback.")
            return None

        # 2. Truy vấn Top 50 bài hát từ view gold_top_tracks
        top_tracks_raw = client.fetch_top_tracks(50)
        # 3. Truy vấn Top 50 nghệ sĩ từ view gold_top_artists
        top_artists_raw = client.fetch_top_artists(50)
        # 4. Truy vấn Phân bố khung giờ từ view gold_listening_schedule
        schedule_raw = client.fetch_listening_schedule()
        # 5. Truy vấn danh sách đầy đủ để tìm kiếm
        all_tracks_raw = client.fetch_all_tracks()
        all_artists_raw = client.fetch_all_artists()
        # 6. Truy vấn lịch sử nghe gần nhất từ silver
        recent_streams_raw = client.fetch_recent_streams(25)

        # Lấy metadata ảnh và link từ cache local
        track_meta, artist_meta = load_local_metadata_lookup()

        # Format Top Tracks
        top_tracks = []
        for row in (top_tracks_raw or []):
            t_id = row.get("track_id")
            meta = track_meta.get(t_id, {})
            streams_cnt = int(row.get("total_streams", 0))
            mins = float(row.get("total_minutes_listened", 0.0))
            img_url = row.get("image_url") or meta.get("image_url", "")
            album = row.get("album_name") or meta.get("album_name", "Spotify Album")
            spot_url = row.get("spotify_url") or meta.get("spotify_url", f"https://open.spotify.com/track/{t_id}")

            top_tracks.append({
                "track_id": t_id,
                "track_name": row.get("track_name", "Unknown Track"),
                "artist_names": row.get("artist_names", "Unknown Artist"),
                "total_streams": streams_cnt,
                "total_minutes": mins,
                "first_listened_at": row.get("first_listened_at"),
                "last_listened_at": row.get("last_listened_at"),
                "image_url": img_url,
                "album_name": album,
                "spotify_url": spot_url
            })

        # Format Top Artists
        top_artists = []
        for row in (top_artists_raw or []):
            a_id = row.get("artist_id")
            meta = artist_meta.get(a_id, {})
            streams_cnt = int(row.get("total_streams", 0))
            mins = float(row.get("total_minutes_listened", 0.0))
            art_img = row.get("sample_image_url") or meta.get("sample_image", "")
            spot_url = row.get("spotify_url") or meta.get("spotify_url", f"https://open.spotify.com/artist/{a_id}")

            top_artists.append({
                "artist_id": a_id,
                "artist_name": row.get("artist_name", "Unknown Artist"),
                "total_streams": streams_cnt,
                "total_minutes": mins,
                "first_listened_at": row.get("first_listened_at"),
                "last_listened_at": row.get("last_listened_at"),
                "sample_image": art_img,
                "spotify_url": spot_url
            })

        # Format All Tracks cho tìm kiếm
        all_tracks = []
        for row in (all_tracks_raw or []):
            t_id = row.get("track_id")
            meta = track_meta.get(t_id, {})
            img_url = row.get("image_url") or meta.get("image_url", "")
            album = row.get("album_name") or meta.get("album_name", "Spotify Album")
            spot_url = row.get("spotify_url") or meta.get("spotify_url", f"https://open.spotify.com/track/{t_id}")

            all_tracks.append({
                "track_id": t_id,
                "track_name": row.get("track_name", "Unknown Track"),
                "artist_names": row.get("artist_names", "Unknown Artist"),
                "total_streams": int(row.get("total_streams", 0)),
                "total_minutes": float(row.get("total_minutes_listened", 0.0)),
                "image_url": img_url,
                "album_name": album,
                "spotify_url": spot_url,
                "is_listened": True
            })

        # Format All Artists cho tìm kiếm
        all_artists = []
        for row in (all_artists_raw or []):
            a_id = row.get("artist_id")
            meta = artist_meta.get(a_id, {})
            art_img = row.get("sample_image_url") or meta.get("sample_image", "")
            spot_url = row.get("spotify_url") or meta.get("spotify_url", f"https://open.spotify.com/artist/{a_id}")

            all_artists.append({
                "artist_id": a_id,
                "artist_name": row.get("artist_name", "Unknown Artist"),
                "total_streams": int(row.get("total_streams", 0)),
                "total_minutes": float(row.get("total_minutes_listened", 0.0)),
                "sample_image": art_img,
                "spotify_url": spot_url
            })

        # Format Thống kê khung giờ
        hourly_distribution = [0] * 24
        time_schedule = {
            "Sáng (05h-12h)": 0,
            "Chiều (12h-18h)": 0,
            "Tối (18h-23h)": 0,
            "Đêm (23h-05h)": 0
        }
        for item in (schedule_raw or []):
            h = int(item.get("hour_of_day", 0))
            count = int(item.get("stream_count", 0))
            slot = item.get("time_slot")
            if 0 <= h < 24:
                hourly_distribution[h] += count
            if slot and slot in time_schedule:
                time_schedule[slot] += count

        # Xác định Persona
        peak_slot = max(time_schedule.items(), key=lambda x: x[1])[0] if time_schedule else "Cân bằng"
        persona = (
            "Cú Đêm Yêu Âm Nhạc 🦉" if "Đêm" in peak_slot else (
                "Người Truyền Năng Lượng Sáng ☀️" if "Sáng" in peak_slot else (
                    "Lofi Buổi Chiều 🌇" if "Chiều" in peak_slot else "Âm Nhạc Buổi Tối 🌙"
                )
            )
        )

        # Format Recent Streams
        recent_streams = []
        for s in (recent_streams_raw or []):
            t_id = s.get("track_id")
            meta = track_meta.get(t_id, {})
            recent_streams.append({
                "played_at": s.get("played_at"),
                "track_id": t_id,
                "track_name": s.get("track_name", "Unknown Track"),
                "artist_names": meta.get("artist_names", ""),
                "image_url": meta.get("image_url", ""),
                "album_name": meta.get("album_name", ""),
                "spotify_url": meta.get("spotify_url", f"https://open.spotify.com/track/{t_id}"),
                "time_slot": s.get("time_slot", ""),
                "hour": int(s.get("hour", 0))
            })

        latest_sync = recent_streams[0]["played_at"] if recent_streams else (
            top_tracks[0].get("last_listened_at") if top_tracks else None
        )

        total_streams = int(summary_row.get("total_streams", 0))
        unique_tracks = int(summary_row.get("unique_tracks", 0))
        unique_artists = int(summary_row.get("unique_artists", 0))
        total_hours = float(summary_row.get("total_hours_listened", 0.0))
        diversity_ratio = float(summary_row.get("diversity_ratio", 0.0))

        # Nạp catalog album & gợi ý bài hát chưa nghe
        listened_track_ids = {t["track_id"] for t in all_tracks}
        artists_dict_lookup = {a["artist_id"]: a for a in all_artists}
        albums_list, unheard_recommendations, unheard_tracks_for_search = load_album_catalog_data(
            listened_track_ids, artists_dict_lookup
        )
        combined_all_tracks = all_tracks + unheard_tracks_for_search

        print(f"✅ [Databricks Lakehouse] Nạp thành công {total_streams} streams, {unique_tracks} tracks, {unique_artists} artists, {len(albums_list)} albums!")

        return {
            "data_source": "databricks_lakehouse",
            "data_source_label": "Databricks Lakehouse (Serverless SQL)",
            "is_live": True,
            "warehouse_name": "Serverless Starter Warehouse",
            "total_streams": total_streams,
            "unique_tracks": unique_tracks,
            "unique_artists": unique_artists,
            "total_hours": total_hours,
            "total_minutes": round(total_hours * 60, 1),
            "diversity_ratio": diversity_ratio,
            "persona": persona,
            "peak_slot": peak_slot,
            "time_schedule": time_schedule,
            "hourly_distribution": hourly_distribution,
            "top_tracks": top_tracks,
            "top_artists": top_artists,
            "all_tracks": combined_all_tracks,
            "all_artists": all_artists,
            "albums": albums_list,
            "unheard_recommendations": unheard_recommendations,
            "recent_streams": recent_streams,
            "latest_sync": latest_sync
        }

    except Exception as e:
        print(f"⚠️ Ngoại lệ khi đọc Databricks Lakehouse: {e}")
        return None


def load_album_catalog_data(listened_track_ids, artists_dict):
    """Đọc file catalog album bronze và tính toán tiến độ hoàn thành album cùng danh sách bài hát chưa nghe gợi ý."""
    catalog_files = sorted(glob.glob(os.path.join(DATA_DIR, "album_catalog_*.json")))
    if not catalog_files:
        return [], [], []

    albums_list = []
    unheard_recommendations = []
    unheard_tracks_for_search = []
    seen_albums = set()
    seen_unheard_track_ids = set()

    for cf in reversed(catalog_files):
        try:
            with open(cf, "r", encoding="utf-8") as f:
                payload = json.load(f)
                albums = payload.get("albums", [])
                for alb in albums:
                    a_id = alb.get("id")
                    if not a_id or a_id in seen_albums:
                        continue
                    seen_albums.add(a_id)

                    a_name = alb.get("name", "Unknown Album")
                    a_type = alb.get("album_type", "album")
                    rel_date = alb.get("release_date", "")
                    images = alb.get("images", [])
                    image_url = images[0]["url"] if images else ""
                    spotify_url = alb.get("external_urls", {}).get("spotify", "")
                    
                    alb_artists = [a.get("name") for a in alb.get("artists", []) if a.get("name")]
                    artist_names_str = ", ".join(alb_artists) if alb_artists else "Various Artists"

                    tracks = alb.get("tracks", {}).get("items", [])
                    total_tracks = alb.get("total_tracks", len(tracks))

                    listened_in_album = 0
                    unheard_in_album = []
                    album_tracklist = []

                    primary_artist_id = alb.get("artists", [{}])[0].get("id", "")
                    artist_priority = artists_dict.get(primary_artist_id, {}).get("total_streams", 0)

                    for tr in tracks:
                        t_id = tr.get("id")
                        t_name = tr.get("name", "Unknown Track")
                        t_num = tr.get("track_number", 1)
                        t_dur = tr.get("duration_ms", 0)
                        t_spot = tr.get("external_urls", {}).get("spotify", "")
                        tr_artists = [a.get("name") for a in tr.get("artists", []) if a.get("name")]
                        tr_artists_str = ", ".join(tr_artists) if tr_artists else artist_names_str

                        is_heard = t_id in listened_track_ids
                        track_item = {
                            "track_id": t_id,
                            "track_name": t_name,
                            "track_number": t_num,
                            "duration_ms": t_dur,
                            "album_id": a_id,
                            "album_name": a_name,
                            "image_url": image_url,
                            "spotify_url": t_spot,
                            "artist_names": tr_artists_str,
                            "is_listened": is_heard,
                            "total_streams": 0
                        }
                        album_tracklist.append(track_item)

                        if is_heard:
                            listened_in_album += 1
                        else:
                            unheard_in_album.append(track_item)
                            if t_id not in seen_unheard_track_ids:
                                seen_unheard_track_ids.add(t_id)
                                rec_item = {
                                    **track_item,
                                    "artist_priority": artist_priority
                                }
                                unheard_recommendations.append(rec_item)
                                unheard_tracks_for_search.append(track_item)

                    comp_rate = round((listened_in_album / total_tracks) * 100.0, 1) if total_tracks > 0 else 0.0

                    albums_list.append({
                        "album_id": a_id,
                        "album_name": a_name,
                        "album_type": a_type,
                        "release_date": rel_date,
                        "artist_names": artist_names_str,
                        "image_url": image_url,
                        "spotify_url": spotify_url,
                        "total_tracks": total_tracks,
                        "tracks_listened": listened_in_album,
                        "unheard_tracks_count": len(unheard_in_album),
                        "completion_rate": comp_rate,
                        "tracklist": album_tracklist
                    })
        except Exception as e:
            print(f"⚠️ Lỗi đọc album catalog {cf}: {e}")

    albums_list.sort(key=lambda x: (x["tracks_listened"], x["completion_rate"]), reverse=True)
    unheard_recommendations.sort(key=lambda x: (x["artist_priority"], -x["track_number"]), reverse=True)

    return albums_list, unheard_recommendations, unheard_tracks_for_search


def load_from_local_bronze():
    """Fallback: Đọc toàn bộ file JSON thô và thực hiện logic tổng hợp local."""
    json_files = glob.glob(os.path.join(DATA_DIR, "personal_*.json"))
    if not json_files:
        return {
            "data_source": "local_json",
            "data_source_label": "Local Bronze JSON (Chưa có dữ liệu)",
            "is_live": False,
            "total_streams": 0,
            "unique_tracks": 0,
            "unique_artists": 0,
            "total_hours": 0.0,
            "total_minutes": 0.0,
            "diversity_ratio": 0.0,
            "persona": "Chưa xác định",
            "peak_slot": "Chưa có",
            "top_tracks": [],
            "top_artists": [],
            "all_tracks": [],
            "all_artists": [],
            "albums": [],
            "unheard_recommendations": [],
            "recent_streams": [],
            "time_schedule": {},
            "hourly_distribution": [0] * 24,
            "latest_sync": None
        }

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

                    stream_key = (played_at_str, track_id)
                    if stream_key in seen_streams:
                        continue
                    seen_streams.add(stream_key)

                    try:
                        dt = datetime.fromisoformat(played_at_str.replace("Z", "+00:00"))
                    except Exception:
                        continue

                    album = track_data.get("album", {})
                    album_images = album.get("images", [])
                    image_url = album_images[0]["url"] if album_images else ""

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
                            "is_listened": True
                        }
                    t_item = tracks_dict[track_id]
                    t_item["total_streams"] += 1
                    if played_at_str < t_item["first_listened_at"]:
                        t_item["first_listened_at"] = played_at_str
                    if played_at_str > t_item["last_listened_at"]:
                        t_item["last_listened_at"] = played_at_str

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

    total_streams_count = len(streams)
    unique_tracks_count = len(tracks_dict)
    unique_artists_count = len(artists_dict)

    total_duration_ms = sum(s["duration_ms"] for s in streams)
    total_hours = round(total_duration_ms / 3600000.0, 2)
    total_minutes = round(total_duration_ms / 60000.0, 1)
    diversity_ratio = round((unique_artists_count / total_streams_count) if total_streams_count > 0 else 0, 3)

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

    all_tracks_sorted = sorted(tracks_dict.values(), key=lambda x: x["total_streams"], reverse=True)
    all_artists_sorted = sorted(artists_dict.values(), key=lambda x: x["total_streams"], reverse=True)

    for t in all_tracks_sorted:
        t["total_minutes"] = round((t["duration_ms"] * t["total_streams"]) / 60000.0, 1)

    for a in all_artists_sorted:
        a["total_minutes"] = round(a["total_duration_ms"] / 60000.0, 1)

    top_tracks = all_tracks_sorted[:50]
    top_artists = all_artists_sorted[:50]

    # Nạp danh mục Album & Gợi ý bài hát chưa nghe
    albums_list, unheard_recommendations, unheard_tracks_for_search = load_album_catalog_data(
        set(tracks_dict.keys()), artists_dict
    )

    # Bổ sung các bài hát chưa nghe vào all_tracks phục vụ tìm kiếm có lọc
    combined_all_tracks = all_tracks_sorted + unheard_tracks_for_search

    peak_slot = max(time_schedule.items(), key=lambda x: x[1])[0] if time_schedule else "Cân bằng"
    persona = "Cú Đêm Yêu Âm Nhạc 🦉" if "Đêm" in peak_slot else ("Người Truyền Năng Lượng Sáng ☀️" if "Sáng" in peak_slot else ("Lofi Buổi Chiều 🌇" if "Chiều" in peak_slot else "Âm Nhạc Buổi Tối 🌙"))
    latest_sync = max((s["played_at"] for s in streams), default=None)

    return {
        "data_source": "local_json",
        "data_source_label": "Local Bronze JSON (Offline Mode)",
        "is_live": False,
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
        "all_tracks": combined_all_tracks,
        "all_artists": all_artists_sorted,
        "albums": albums_list,
        "unheard_recommendations": unheard_recommendations,
        "recent_streams": sorted(streams, key=lambda x: x["played_at"], reverse=True)[:25],
        "latest_sync": latest_sync
    }


def get_dashboard_data(force_refresh=False):
    """Lấy dữ liệu cho Dashboard: Ưu tiên Databricks Lakehouse, fallback sang Local JSON."""
    now = time.time()
    if not force_refresh and CACHE_STATE["data"] and (now - CACHE_STATE["cached_at"]) < CACHE_STATE["ttl_seconds"]:
        return CACHE_STATE["data"]

    client = DatabricksLakehouseClient()
    data = None
    if client.is_configured():
        data = load_from_databricks_lakehouse(client)

    if not data:
        print("📁 Sử dụng nguồn dữ liệu Local Bronze JSON dự phòng.")
        data = load_from_local_bronze()

    CACHE_STATE["data"] = data
    CACHE_STATE["cached_at"] = now
    return data


class SpotifyAppHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=WEB_DIR, **kwargs)

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path

        if path == "/api/stats":
            query_params = urllib.parse.parse_qs(parsed.query)
            force_refresh = query_params.get("refresh", ["false"])[0].lower() == "true"
            data = get_dashboard_data(force_refresh=force_refresh)
            
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps(data, ensure_ascii=False).encode("utf-8"))
            return

        elif path == "/api/albums":
            data = get_dashboard_data(force_refresh=False)
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps(data.get("albums", []), ensure_ascii=False).encode("utf-8"))
            return

        elif path == "/api/recommendations":
            data = get_dashboard_data(force_refresh=False)
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps(data.get("unheard_recommendations", []), ensure_ascii=False).encode("utf-8"))
            return

        elif path == "/api/search":
            query_params = urllib.parse.parse_qs(parsed.query)
            q = query_params.get("q", [""])[0].strip().lower()
            filter_mode = query_params.get("filter", ["all"])[0].lower()
            data = get_dashboard_data(force_refresh=False)

            source_tracks = data.get("all_tracks", [])
            if filter_mode == "heard":
                source_tracks = [t for t in source_tracks if t.get("is_listened") == True]
            elif filter_mode == "unheard":
                source_tracks = [t for t in source_tracks if t.get("is_listened") == False]

            if not q:
                results = {
                    "matched_tracks": source_tracks[:30],
                    "matched_artists": data["all_artists"][:20]
                }
            else:
                matched_tracks = [
                    t for t in source_tracks
                    if q in t["track_name"].lower() or q in t["artist_names"].lower() or q in t.get("album_name", "").lower()
                ][:40]
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

        elif path == "/api/status":
            client = DatabricksLakehouseClient()
            ok, msg = client.test_connection()
            status_data = {
                "connected": ok,
                "message": msg,
                "host": client.host,
                "warehouse_id": client.warehouse_id,
                "current_source": CACHE_STATE["data"]["data_source"] if CACHE_STATE["data"] else "unknown"
            }
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps(status_data, ensure_ascii=False).encode("utf-8"))
            return

        # Phục vụ file tĩnh từ thư mục web/
        return super().do_GET()

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path == "/api/sync":
            try:
                print("🔄 [API] Kích hoạt cào dữ liệu mới từ Spotify API...")
                python_exe = sys.executable
                script_path = os.path.join(os.path.dirname(__file__), "src", "ingest_personal.py")
                res = subprocess.run([python_exe, script_path], capture_output=True, text=True, timeout=60)
                
                success = res.returncode == 0
                if success:
                    # Xoá cache để lần tải tiếp theo lấy dữ liệu mới nhất
                    CACHE_STATE["data"] = None
                    CACHE_STATE["cached_at"] = 0

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

        elif parsed.path == "/api/refresh-lakehouse":
            # Làm mới cache ngay lập tức từ Databricks Lakehouse
            data = get_dashboard_data(force_refresh=True)
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(json.dumps({"success": True, "source": data["data_source"]}, ensure_ascii=False).encode("utf-8"))
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
    print(f"⚡ Kết nối Databricks: Serverless Starter Warehouse")
    print(f"✨ Mở trình duyệt và trải nghiệm ngay!")
    print(f"=======================================================\n")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n🛑 Đã dừng máy chủ.")
        httpd.server_close()


if __name__ == "__main__":
    run_server()
