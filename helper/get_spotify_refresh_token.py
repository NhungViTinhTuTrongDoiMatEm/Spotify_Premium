"""
Script hỗ trợ lấy Spotify OAuth 2.0 Refresh Token 1-lần (One-time setup).
Chạy script này dưới local, đăng nhập Spotify trên trình duyệt để cấp quyền.
"""

import os
import sys
import webbrowser
import urllib.parse
from http.server import HTTPServer, BaseHTTPRequestHandler
import requests
from dotenv import load_dotenv

load_dotenv()

# 2. Lấy Client ID và Client Secret từ môi trường hoặc yêu cầu người dùng nhập vào
CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID") or input("Nhập Spotify CLIENT_ID: ").strip()
CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET") or input("Nhập Spotify CLIENT_SECRET: ").strip()

# Redirect URI trùng khớp chính xác với những gì bạn điền trên Spotify Developer Dashboard
REDIRECT_URI = "http://127.0.0.1:8888/callback"
# Phạm vi quyền truy cập cần xin người dùng (lịch sử nghe gần đây & top bài hát)
SCOPE = "user-read-recently-played user-top-read"

auth_code = None

# 3. Tạo HTTP Server siêu nhỏ ở local để 'hứng' Callback từ Spotify gửi về
class OAuthCallbackHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        global auth_code
        parsed_url = urllib.parse.urlparse(self.path)
        query_params = urllib.parse.parse_qs(parsed_url.query)

        # Nếu Spotify trả về parameter ?code=...
        if "code" in query_params:
            auth_code = query_params["code"][0]
            self.send_response(200)
            self.send_header("Content-type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(
                """
                <html>
                <body style='font-family: sans-serif; text-align: center; padding-top: 50px;'>
                    <h1 style='color: #1DB954;'>Xác thực thành công!</h1>
                    <p>Bạn có thể đóng cửa sổ trình duyệt này và quay lại terminal.</p>
                </body>
                </html>
                """.encode("utf-8")
            )
        else:
            self.send_response(400)
            self.end_headers()

    def log_message(self, format, *args):
        return  # Tắt bớt log HTTP mặc định để terminal sạch đẹp

def run_oauth_flow():
    if not CLIENT_ID or not CLIENT_SECRET:
        print("❌ Lỗi: Bạn phải nhập đầy đủ CLIENT_ID và CLIENT_SECRET!")
        sys.exit(1)

    # 4. Tạo URL đăng nhập Spotify với Client ID và Scope đã cấu hình
    auth_url = (
        "https://accounts.spotify.com/authorize?"
        + urllib.parse.urlencode({
            "client_id": CLIENT_ID,
            "response_type": "code",
            "redirect_uri": REDIRECT_URI,
            "scope": SCOPE,
        })
    )

    print("\n------------------------------------------------------------")
    print("🚀 Đang mở trình duyệt để bạn đăng nhập Spotify...")
    print(f"🔗 URL: {auth_url}")
    print("------------------------------------------------------------\n")

    # Tự động mở trang đăng nhập Spotify trên trình duyệt mặc định
    webbrowser.open(auth_url)

    # Khởi chạy HTTP Server tại cổng 8888 để chờ kết quả
    server_address = ("127.0.0.1", 8888)
    httpd = HTTPServer(server_address, OAuthCallbackHandler)
    print("⏳ Đang chờ bạn bấm 'Agree' trên trình duyệt...")
    
    # Lắng nghe 1 request duy nhất cho tới khi có Authorization Code
    while auth_code is None:
        httpd.handle_request()

    print("\n✅ Đã nhận Authorization Code thành công!")
    print("🔄 Đang gửi request tới Spotify API để đổi lấy Refresh Token...")

    # 5. Gửi POST request tới Spotify token endpoint để đổi Authorization Code lấy Refresh Token
    token_url = "https://accounts.spotify.com/api/token"
    payload = {
        "grant_type": "authorization_code",
        "code": auth_code,
        "redirect_uri": REDIRECT_URI,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }

    response = requests.post(token_url, data=payload)
    if response.status_code == 200:
        data = response.json()
        refresh_token = data.get("refresh_token")

        print("\n============================================================")
        print("🎉 THÀNH CÔNG! ĐÃ LẤY ĐƯỢC SPOTIFY REFRESH TOKEN")
        print("============================================================")
        print(f"🔑 REFRESH_TOKEN: {refresh_token}")
        print("============================================================\n")

        # 6. Tự động lưu hoặc cập nhật thông tin bí mật vào file .env ở local
        with open(".env", "w", encoding="utf-8") as f:
            f.write(f"SPOTIFY_CLIENT_ID={CLIENT_ID}\n")
            f.write(f"SPOTIFY_CLIENT_SECRET={CLIENT_SECRET}\n")
            f.write(f"SPOTIFY_REFRESH_TOKEN={refresh_token}\n")

        print("💾 Đã tự động lưu thông tin vào file `.env` bảo mật ở local.")
    else:
        print(f"❌ Lỗi lấy Refresh Token ({response.status_code}): {response.text}")

if __name__ == "__main__":
    run_oauth_flow()
