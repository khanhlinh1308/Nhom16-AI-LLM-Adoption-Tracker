# 📊 AI/LLM Adoption Tracker — Nhóm 16

Hệ thống Big Data theo dõi xu hướng ứng dụng AI/LLM trong các dự án mã nguồn mở trên GitHub theo **thời gian thực**.

🎬 **[Click để xem Video Demo Hệ thống hoạt động](https://drive.google.com/file/d/1ysJgwIrwS9_LkdI2uUhSxS8cLeZNuYlw/view?usp=sharing)**

```
GitHub Events API ➔ Python Collector ➔ Apache Kafka ➔ Apache Spark (Structured Streaming) ➔ Redis ➔ Streamlit Dashboard
```

---

## 1. Yêu cầu hệ thống

| Thành phần | Yêu cầu tối thiểu |
|---|---|
| RAM | 8GB (khuyến nghị 16GB+ vì Spark Streaming khá nặng) |
| Ổ đĩa trống | 15–20GB (Docker image) |
| Docker & Docker Compose | Bản mới nhất |
| Java (JVM) | 8 / 11 / 17 (bắt buộc cho Spark) |
| Python | 3.8 – 3.11 |
| Git | bất kỳ bản mới |

Kiểm tra nhanh máy đã sẵn sàng chưa (PowerShell, chạy với quyền Admin):

```powershell
docker --version
docker compose version
java -version
git --version
```

---

## 2. Cấu trúc dự án

```
Nhom16-AI-LLM-Adoption-Tracker/
├── .env
├── docker-compose.yml
├── README.md
├── requirements.txt
├── data/
│   └── sample_dataset.json
├── docs/
│   ├── report/
│   └── slide/
└── src/
    ├── ingestion/
    │   └── github_collector.py
    ├── processing/
    │   └── spark_streaming.py
    └── visualization/
        └── app.py
```

---

## 3. Cài đặt

### 3.1. Clone repo

```bash
git clone https://github.com/khanhlinh1308/Nhom16-AI-LLM-Adoption-Tracker.git
cd Nhom16-AI-LLM-Adoption-Tracker
```

### 3.2. Tạo file `.env`

```env
KAFKA_BROKER=kafka:9092
KAFKA_TOPIC=github_events
REDIS_HOST=redis
REDIS_PORT=6379
POLL_INTERVAL_SECONDS=30
GITHUB_TOKEN=
WINDOW_DURATION=1 minute
SLIDE_DURATION=1 minute
WATERMARK_DELAY=2 minutes
```

> 💡 `GITHUB_TOKEN` không bắt buộc, nhưng nên tạo một [Personal Access Token](https://github.com/settings/tokens) để tăng rate limit từ 60 lên 5000 request/giờ.

### 3.3. Cài thư viện Python (nếu muốn chạy/test cục bộ ngoài Docker)

```bash
python -m venv venv
source venv/bin/activate      # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

---

## 4. Khởi chạy toàn bộ hệ thống bằng Docker

```bash
docker compose up -d
```

Lệnh này sẽ tự khởi tạo các service: **Zookeeper, Kafka, Spark Master/Worker, Redis, Streamlit**.

Kiểm tra các container đang chạy:

```bash
docker compose ps
```

Xem log của từng service khi cần debug:

```bash
docker compose logs -f github_collector
docker compose logs -f spark_streaming
docker compose logs -f streamlit
```

Dừng toàn bộ hệ thống:

```bash
docker compose down
```

---

## 5. Truy cập Dashboard & Tính năng nổi bật

Sau khi `docker compose up -d` chạy thành công (đợi khoảng 30–60 giây để Kafka/Spark khởi động và gom mẻ Windowing đầu tiên), mở trình duyệt tại:
http://localhost:8501


**Các tính năng UI/UX nổi bật:**
- **Đa ngôn ngữ (i18n):** Hỗ trợ chuyển đổi trực tiếp giữa Tiếng Anh (EN) và Tiếng Việt (VN) trên Sidebar mà không cần tải lại trang.
- **Smart Auto-refresh:** Tích hợp bộ đếm ngược thời gian thực (hiển thị từng giây). Người dùng có thể Tắt/Bật công tắc này để tạm dừng làm mới khi cần thao tác với bộ lọc (Filters).
- **Interactive Filters:** Bộ lọc đa luồng theo công nghệ AI (ChatGPT, RAG, LangChain...) và các bảng dữ liệu tích hợp link truy cập thẳng đến kho lưu trữ GitHub gốc.

---

## 6. Chạy thủ công từng thành phần (không qua Docker — dùng để dev/debug)

### 6.1. Producer — thu thập dữ liệu GitHub

```bash
cd src/ingestion
python github_collector.py
```

### 6.2. Consumer — xử lý luồng bằng Spark

```bash
cd src/processing
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  spark_streaming.py
```

### 6.3. Dashboard

```bash
cd src/visualization
streamlit run app.py
```

---

## 7. Phương án dự phòng (nếu Demo gặp lỗi mạng/API)

Dự án có sẵn dữ liệu mẫu tại `data/sample_dataset.json` để test luồng Spark mà không cần gọi GitHub API thật — phòng trường hợp rate limit hoặc mất mạng lúc báo cáo.

Nếu Spark gặp khó khăn khi ghi vào Redis, có thể chuyển sang ghi tạm ra CSV (`writeStream.format("csv")`) và để Streamlit đọc từ file CSV thay vì Redis.

---

## 8. Các chỉ số chính trên Dashboard

- **Total AI Repos Detected** — tổng số repo liên quan AI mới phát hiện
- **Total AI Commits** — tổng số commit chứa từ khóa AI
- **Top Trending Technology** — công nghệ AI được nhắc nhiều nhất trong khung giờ hiện tại
- **AI Activity over Time** (Line chart) — biến động hoạt động theo thời gian
- **Top AI Technologies** (Bar chart) — xếp hạng tần suất từ khóa
- **Latest AI Repositories** (bảng) — danh sách repo mới bắt được từ Kafka, click để mở GitHub gốc

Các chỉ số này được Spark tính toán theo cửa sổ thời gian (mặc định 1 phút) và lưu trong Redis với các key: `top_ai_keywords`, `top_ai_repos`, `total_ai_events_detected`, `ai_activity_timeline`.

---

## 9. Đội ngũ thực hiện

| Thành viên | Vai trò |
|---|---|
| Linh | Project Lead, Tài liệu đặc tả, UI/UX Dashboard, Báo cáo & Slide, Video demo |
| Giang | Core Data Engineer — GitHub Collector, Kafka, PySpark Streaming, Hạ tầng (Docker) |

---

## 10. Tài liệu liên quan

- Báo cáo chi tiết: `docs/report/BaoCao_Nhom16.pdf`
- Slide thuyết trình: `docs/slide/Slide_Nhom16.pdf`
- Video Demo: [Xem trực tiếp trên Google Drive](https://drive.google.com/file/d/1ysJgwIrwS9_LkdI2uUhSxS8cLeZNuYlw/view?usp=sharing)
