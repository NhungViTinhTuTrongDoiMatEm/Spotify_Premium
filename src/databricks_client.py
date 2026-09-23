"""
Databricks Lakehouse SQL Client
Kết nối và truy vấn dữ liệu từ Databricks Serverless SQL Warehouse qua REST Statement Execution API.
Tự động lấy thông tin từ .env và hỗ trợ timeout / polling / fallback.
"""

import os
import sys
import time
import requests
from typing import Dict, List, Any, Optional, Tuple

# Cấu hình UTF-8 cho Windows console
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

ENV_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")


def load_env_vars() -> Dict[str, str]:
    """Đọc các biến môi trường từ .env an toàn không cần phụ thuộc thư viện bên ngoài."""
    env_vars = {}
    if os.path.exists(ENV_FILE):
        with open(ENV_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, val = line.split("=", 1)
                    env_vars[key.strip()] = val.strip()
    return env_vars


class DatabricksLakehouseClient:
    def __init__(self):
        env_vars = load_env_vars()
        self.host = env_vars.get("DATABRICKS_HOST", "").rstrip("/")
        self.token = env_vars.get("DATABRICKS_TOKEN", "")
        self.warehouse_id = env_vars.get("DATABRICKS_WAREHOUSE_ID", "4314b4f0abd4f131")
        self.headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }

    def is_configured(self) -> bool:
        return bool(self.host and self.token and self.warehouse_id)

    def execute_sql(self, sql_statement: str, timeout_seconds: int = 30) -> Optional[List[Dict[str, Any]]]:
        """Thực thi câu lệnh SQL trên Databricks Serverless Warehouse và trả về danh sách đối tượng dict."""
        if not self.is_configured():
            print("⚠️ Databricks chưa được cấu hình đầy đủ trong .env")
            return None

        url = f"{self.host}/api/2.0/sql/statements"
        payload = {
            "warehouse_id": self.warehouse_id,
            "statement": sql_statement,
            "wait_timeout": f"{min(timeout_seconds, 50)}s"
        }

        try:
            res = requests.post(url, headers=self.headers, json=payload, timeout=timeout_seconds + 5)
            if res.status_code != 200:
                print(f"❌ Lỗi truy vấn Databricks (Status {res.status_code}): {res.text[:300]}")
                return None

            data = res.json()
            statement_id = data.get("statement_id")
            state = data.get("status", {}).get("state")

            # Nếu câu lệnh đang chạy (chẳng hạn warehouse đang khởi động), thăm dò tiếp tối đa timeout
            start_poll = time.time()
            while state in ("PENDING", "RUNNING") and (time.time() - start_poll) < timeout_seconds:
                time.sleep(2)
                check_url = f"{self.host}/api/2.0/sql/statements/{statement_id}"
                c_res = requests.get(check_url, headers=self.headers, timeout=10)
                if c_res.status_code == 200:
                    data = c_res.json()
                    state = data.get("status", {}).get("state")
                else:
                    break

            if state != "SUCCEEDED":
                error_msg = data.get("status", {}).get("error", {}).get("message", "Unknown error")
                print(f"❌ Lỗi thực thi SQL trên Databricks (State {state}): {error_msg}")
                return None

            # Parse schema và dữ liệu
            columns = [col["name"] for col in data.get("manifest", {}).get("schema", {}).get("columns", [])]
            data_array = data.get("result", {}).get("data_array", [])

            rows = []
            for item in data_array:
                row_dict = {}
                for idx, col_name in enumerate(columns):
                    val = item[idx] if idx < len(item) else None
                    row_dict[col_name] = val
                rows.append(row_dict)

            return rows

        except Exception as e:
            print(f"⚠️ Ngoại lệ khi kết nối Databricks: {e}")
            return None

    def test_connection(self) -> Tuple[bool, str]:
        """Kiểm tra kết nối và trạng thái Warehouse."""
        if not self.is_configured():
            return False, "Thiếu thông tin cấu hình DATABRICKS_HOST, DATABRICKS_TOKEN, hoặc DATABRICKS_WAREHOUSE_ID trong .env"
        try:
            check_url = f"{self.host}/api/2.0/sql/endpoints/{self.warehouse_id}"
            res = requests.get(check_url, headers=self.headers, timeout=10)
            if res.status_code == 200:
                ep_data = res.json()
                name = ep_data.get("name", "Unknown Warehouse")
                state = ep_data.get("state", "UNKNOWN")
                return True, f"Kết nối thành công tới '{name}' (Trạng thái: {state})"
            else:
                return False, f"Không tìm thấy Warehouse {self.warehouse_id} (Status {res.status_code})"
        except Exception as e:
            return False, f"Lỗi mạng: {e}"

    def fetch_listening_summary(self) -> Optional[Dict[str, Any]]:
        """Lấy chỉ số KPI từ view spotify_gold.gold_listening_summary."""
        rows = self.execute_sql("SELECT * FROM spotify_gold.gold_listening_summary")
        if rows and len(rows) > 0:
            return rows[0]
        return None

    def fetch_top_tracks(self, limit: int = 50) -> Optional[List[Dict[str, Any]]]:
        """Lấy top bài hát nghe nhiều nhất từ view spotify_gold.gold_top_tracks."""
        return self.execute_sql(f"SELECT * FROM spotify_gold.gold_top_tracks LIMIT {limit}")

    def fetch_top_artists(self, limit: int = 50) -> Optional[List[Dict[str, Any]]]:
        """Lấy top nghệ sĩ từ view spotify_gold.gold_top_artists."""
        return self.execute_sql(f"SELECT * FROM spotify_gold.gold_top_artists LIMIT {limit}")

    def fetch_listening_schedule(self) -> Optional[List[Dict[str, Any]]]:
        """Lấy phân bố khung giờ nghe nhạc từ view spotify_gold.gold_listening_schedule."""
        return self.execute_sql("SELECT * FROM spotify_gold.gold_listening_schedule ORDER BY hour_of_day ASC")

    def fetch_all_tracks(self) -> Optional[List[Dict[str, Any]]]:
        """Lấy toàn bộ track từ agg_track_metrics phục vụ tìm kiếm."""
        return self.execute_sql("SELECT * FROM spotify_gold.agg_track_metrics ORDER BY total_streams DESC")

    def fetch_all_artists(self) -> Optional[List[Dict[str, Any]]]:
        """Lấy toàn bộ nghệ sĩ từ agg_artist_metrics phục vụ tìm kiếm."""
        return self.execute_sql("SELECT * FROM spotify_gold.agg_artist_metrics ORDER BY total_streams DESC")

    def fetch_recent_streams(self, limit: int = 25) -> Optional[List[Dict[str, Any]]]:
        """Lấy danh sách các lượt nghe gần nhất từ tầng Silver."""
        query = f"""
        SELECT 
          f.played_at,
          f.track_id,
          t.track_name,
          t.duration_ms,
          f.time_slot,
          hour(f.played_at) AS hour
        FROM spotify_silver.fact_streams f
        JOIN spotify_silver.dim_tracks t ON f.track_id = t.track_id
        ORDER BY f.played_at DESC
        LIMIT {limit}
        """
        return self.execute_sql(query)


# Test độc lập
if __name__ == "__main__":
    client = DatabricksLakehouseClient()
    ok, msg = client.test_connection()
    print("Test kết nối:", ok, msg)
    if ok:
        summary = client.fetch_listening_summary()
        print("Summary:", summary)
        top_tracks = client.fetch_top_tracks(3)
        print("Top 3 tracks count:", len(top_tracks) if top_tracks else 0)
