# 📚 Hướng dẫn Triển khai Personal Spotify Analytics Lakehouse

## 1. Đẩy Mã nguồn lên GitHub Repository
Mở Terminal tại local và chạy các lệnh sau để đẩy code lên kho lưu trữ GitHub của bạn:
```bash
git init
git add .
git commit -m "Feat: Complete Personal Spotify Analytics Lakehouse Pipeline"
git branch -M main
git remote add origin https://github.com/TuanHungNguyen88/Spotify_Premium.git
git push -u origin main
```

---

## 2. Cấu hình GitHub Repository Secrets (Để tự động hoá ngầm 24/7)
Vào GitHub Repo của bạn (`https://github.com/TuanHungNguyen88/Spotify_Premium`) -> **Settings** -> **Secrets and variables** -> **Actions** -> Bấm **New repository secret** và thêm 5 biến bảo mật sau:

1. `SPOTIFY_CLIENT_ID`: Client ID từ Spotify Developer Dashboard.
2. `SPOTIFY_CLIENT_SECRET`: Client Secret từ Spotify Developer Dashboard.
3. `SPOTIFY_REFRESH_TOKEN`: Mã Refresh Token sinh ra từ script local.
4. `DATABRICKS_HOST`: URL Databricks Workspace (ví dụ: `https://community.cloud.databricks.com`).
5. `DATABRICKS_TOKEN`: Access Token kết nối Databricks (*Vào Databricks -> User Settings -> Developer -> Access Tokens -> Generate new token*).

---

## 3. Đồng bộ Repo vào Databricks Workspace
1. Đăng nhập vào **Databricks Community Edition**.
2. Bên thanh menu trái, chọn **Workspace** -> **Repos** -> Bấm **Add Repo**.
3. Dán URL GitHub Repo (`https://github.com/TuanHungNguyen88/Spotify_Premium.git`) vào và bấm **Create Repo**.

---

## 4. Chạy Pipeline Xử lý Dữ liệu trên Databricks
1. Mở Notebook `notebooks/01_silver_personal.py` -> Bấm **Run All** để làm sạch dữ liệu Bronze thành Silver.
2. Mở Notebook `notebooks/02_gold_personal.py` -> Bấm **Run All** để tính toán chỉ số Gold.

---

## 5. Dựng Databricks Lakeview Dashboard
1. Vào mục **Dashboards** -> Bấm **Create Lakeview Dashboard**.
2. Thêm các Widget biểu đồ:
   - **Scorecard Metric**: Thêm các chỉ số từ View `spotify_gold.gold_listening_summary` (`total_hours_listened`, `unique_tracks`, `diversity_ratio`).
   - **Bar Chart (Top Tracks)**: Trục X là `track_name`, Trục Y là `total_streams` từ `spotify_gold.gold_top_tracks`.
   - **Bar Chart (Top Artists)**: Trục X là `artist_name`, Trục Y là `total_streams` từ `spotify_gold.gold_top_artists`.
   - **Heatmap (Khung giờ nghe)**: Trục X là `hour_of_day`, Trục Y là `day_of_week_name`, Giá trị màu là `stream_count` từ `spotify_gold.gold_listening_schedule`.
