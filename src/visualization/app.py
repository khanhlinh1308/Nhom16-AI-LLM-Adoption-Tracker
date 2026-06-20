"""Streamlit dashboard for Redis metrics produced by Spark Streaming."""

import json
import os
import time
from datetime import datetime

import pandas as pd
import redis
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REFRESH_SECONDS = int(os.getenv("DASHBOARD_REFRESH_SECONDS", "5"))

st.set_page_config(page_title="AI/LLM Adoption Tracker", page_icon="📊", layout="wide")

@st.cache_resource
def redis_client() -> redis.Redis:
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True, socket_connect_timeout=2)

def zset_to_frame(client: redis.Redis, key: str, label: str, limit: int = 10) -> pd.DataFrame:
    rows = client.zrevrange(key, 0, limit - 1, withscores=True)
    return pd.DataFrame([{label: name, "count": int(score)} for name, score in rows])

def timeline_to_frame(client: redis.Redis, limit: int = 100) -> pd.DataFrame:
    rows = client.lrange("ai_activity_timeline", 0, limit - 1)
    snapshots = []
    for row in rows:
        try:
            item = json.loads(row)
            snapshots.append({
                "timestamp": pd.to_datetime(item.get("timestamp")),
                "ai_event_count": int(item.get("ai_event_count", 0)),
                "batch_id": item.get("batch_id"),
            })
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
    if not snapshots:
        return pd.DataFrame(columns=["timestamp", "ai_event_count", "batch_id"])
    return pd.DataFrame(snapshots).sort_values("timestamp")

# --- TRANSLATION DICTIONARY ---
LANG = {
    "EN": {
        "title": "📊 AI/LLM Adoption Tracker",
        "caption": "Realtime GitHub Events API ➔ Kafka ➔ Spark Structured Streaming ➔ Redis",
        "ctrl_header": "⚙️ Dashboard Controls",
        "auto_refresh": f"🔄 Auto-refresh ({REFRESH_SECONDS}s)",
        "auto_refresh_help": "Uncheck to freeze data and apply filters without interruption",
        "filter_tech": "Filter by Technology:",
        "filter_help": "Select specific AI technologies to filter the Bar Chart.",
        "kpi_total": "🔥 Total AI Events",
        "kpi_batch": "⚡ Latest Batch",
        "kpi_top": "👑 Top Technology",
        "kpi_update": "⏱️ Last Update",
        "events_word": "events",
        "empty_state": "No Redis metrics yet. Start the pipeline with `docker compose up -d` and wait for GitHub events to arrive.",
        "chart_tech": "Top AI Technologies",
        "chart_repo": "Top AI Repositories",
        "no_tech_data": "No keyword data yet.",
        "no_repo_data": "No repository data yet.",
        "repo_col": "Repository Name",
        "count_col": "Event Count",
        "link_col": "GitHub Link",
        "open_repo": "Open Repo ↗",
        "chart_time": "AI Activity Over Time",
        "no_time_data": "No timeline data yet.",
        "rendered": "Rendered at",
        "refreshing": "Refreshing data in",
        "seconds": "seconds"
    },
    "VN": {
        "title": "📊 Hệ thống Theo dõi Ứng dụng AI/LLM",
        "caption": "Luồng dữ liệu: GitHub Events API ➔ Kafka ➔ Spark Structured Streaming ➔ Redis",
        "ctrl_header": "⚙️ Bảng Điều Khiển",
        "auto_refresh": f"🔄 Tự động làm mới ({REFRESH_SECONDS}s)",
        "auto_refresh_help": "Tắt để giữ nguyên dữ liệu và thao tác bộ lọc không bị gián đoạn",
        "filter_tech": "Lọc theo công nghệ:",
        "filter_help": "Chọn các từ khoá AI cụ thể để lọc trên Biểu đồ Cột.",
        "kpi_total": "🔥 Tổng Sự Kiện AI",
        "kpi_batch": "⚡ Mẻ Dữ Liệu Gần Nhất",
        "kpi_top": "👑 Công Nghệ Top 1",
        "kpi_update": "⏱️ Cập Nhật Lần Cuối",
        "events_word": "sự kiện",
        "empty_state": "Chưa có dữ liệu từ Redis. Hãy chạy `docker compose up -d` và chờ sự kiện từ GitHub đổ về.",
        "chart_tech": "Top Các Công Nghệ AI",
        "chart_repo": "Top Các Kho Lưu Trữ (Repo) AI",
        "no_tech_data": "Chưa có dữ liệu từ khoá.",
        "no_repo_data": "Chưa có dữ liệu kho lưu trữ.",
        "repo_col": "Tên Repo",
        "count_col": "Số Lượt Nhắc",
        "link_col": "Đường Dẫn GitHub",
        "open_repo": "Mở Repo ↗",
        "chart_time": "Hoạt Động Của AI Theo Thời Gian",
        "no_time_data": "Chưa có chuỗi thời gian.",
        "rendered": "Kết xuất lúc",
        "refreshing": "Làm mới dữ liệu sau",
        "seconds": "giây"
    }
}

# --- SIDEBAR (Filters & Language) ---
with st.sidebar:
    st.image("https://github.githubassets.com/images/modules/logos_page/GitHub-Mark.png", width=60)
    
    # NÚT CHỌN NGÔN NGỮ
    selected_lang = st.radio("🌐 Language / Ngôn ngữ", ["EN", "VN"], horizontal=True)
    t = LANG[selected_lang] # Biến t chứa bộ ngôn ngữ được chọn
    
    st.divider()
    
    st.header(t["ctrl_header"])
    
    # Checkbox Toggle
    auto_refresh = st.checkbox(t["auto_refresh"], value=True, help=t["auto_refresh_help"])
    
    try:
        client = redis_client()
        client.ping()
    except redis.RedisError as exc:
        st.error(f"Cannot connect to Redis at {REDIS_HOST}:{REDIS_PORT}: {exc}")
        st.stop()
        
    keywords_df = zset_to_frame(client, "top_ai_keywords", "keyword")
    
    # Technology Filter
    tech_options = keywords_df["keyword"].tolist() if not keywords_df.empty else []
    selected_tech = st.multiselect(
        t["filter_tech"],
        options=tech_options,
        default=[],
        help=t["filter_help"]
    )

# --- MAIN UI ---
st.title(t["title"])
st.caption(t["caption"])

# --- FETCH DATA ---
total_events = int(client.get("total_ai_events_detected") or 0)
repos_df = zset_to_frame(client, "top_ai_repos", "repository")
timeline_df = timeline_to_frame(client, limit=1000)
top_keyword = keywords_df.iloc[0]["keyword"] if not keywords_df.empty else "-"
latest_count = int(timeline_df.iloc[-1]["ai_event_count"]) if not timeline_df.empty else 0
last_update = timeline_df.iloc[-1]["timestamp"] if not timeline_df.empty else None
last_update_text = last_update.strftime("%Y-%m-%d %H:%M:%S UTC") if last_update is not None else "-"

# --- KPI METRICS ---
metric_cols = st.columns(4)
metric_cols[0].metric(t["kpi_total"], total_events)
metric_cols[1].metric(t["kpi_batch"], latest_count, delta=f"+{latest_count} {t['events_word']}" if latest_count > 0 else None)
metric_cols[2].metric(t["kpi_top"], top_keyword)
metric_cols[3].metric(t["kpi_update"], last_update_text)

if total_events == 0 and keywords_df.empty and repos_df.empty and timeline_df.empty:
    st.info(t["empty_state"])

# --- CHARTS & TABLES ---
left, right = st.columns(2)

with left:
    st.subheader(t["chart_tech"])
    if keywords_df.empty:
        st.write(t["no_tech_data"])
    else:
        filtered_keywords_df = keywords_df.copy()
        if selected_tech:
            filtered_keywords_df = filtered_keywords_df[filtered_keywords_df["keyword"].isin(selected_tech)]
        st.bar_chart(filtered_keywords_df.set_index("keyword"))

with right:
    st.subheader(t["chart_repo"])
    if repos_df.empty:
        st.write(t["no_repo_data"])
    else:
        repos_df["url"] = "https://github.com/" + repos_df["repository"]
        st.dataframe(
            repos_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "repository": t["repo_col"],
                "count": t["count_col"],
                "url": st.column_config.LinkColumn(t["link_col"], display_text=t["open_repo"])
            }
        )

st.subheader(t["chart_time"])
if timeline_df.empty:
    st.write(t["no_time_data"])
else:
    st.line_chart(timeline_df.set_index("timestamp")[["ai_event_count"]])

st.caption(f"{t['rendered']} {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}.")

# --- NATIVE AUTO-REFRESH LỘT XÁC ---
if auto_refresh:
    countdown_placeholder = st.empty()
    
    for i in range(REFRESH_SECONDS, 0, -1):
        countdown_placeholder.caption(f"⏳ {t['refreshing']} {i} {t['seconds']}...")
        time.sleep(1)
        
    countdown_placeholder.empty()
    
    if hasattr(st, "rerun"):
        st.rerun()
    else:
        st.experimental_rerun()
