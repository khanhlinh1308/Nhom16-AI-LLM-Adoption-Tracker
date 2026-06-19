"""Streamlit dashboard for Redis metrics produced by Spark Streaming."""

import json
import os
from datetime import datetime

import pandas as pd
import redis
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REFRESH_SECONDS = int(os.getenv("DASHBOARD_REFRESH_SECONDS", "5"))

st.set_page_config(page_title="AI/LLM Adoption Tracker", layout="wide")
st.markdown(f"<meta http-equiv='refresh' content='{REFRESH_SECONDS}'>", unsafe_allow_html=True)


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


def render_empty_state() -> None:
    st.info("No Redis metrics yet. Start the pipeline with `docker compose up -d` and wait for GitHub events to arrive.")


st.title("AI/LLM Adoption Tracker")
st.caption("Realtime GitHub Events API -> Kafka -> Spark Structured Streaming -> Redis")

try:
    client = redis_client()
    client.ping()
except redis.RedisError as exc:
    st.error(f"Cannot connect to Redis at {REDIS_HOST}:{REDIS_PORT}: {exc}")
    st.stop()

total_events = int(client.get("total_ai_events_detected") or 0)
keywords_df = zset_to_frame(client, "top_ai_keywords", "keyword")
repos_df = zset_to_frame(client, "top_ai_repos", "repository")
timeline_df = timeline_to_frame(client)

top_keyword = keywords_df.iloc[0]["keyword"] if not keywords_df.empty else "-"
latest_count = int(timeline_df.iloc[-1]["ai_event_count"]) if not timeline_df.empty else 0
last_update = timeline_df.iloc[-1]["timestamp"] if not timeline_df.empty else None
last_update_text = last_update.strftime("%Y-%m-%d %H:%M:%S UTC") if last_update is not None else "-"

metric_cols = st.columns(4)
metric_cols[0].metric("Total AI Events", total_events)
metric_cols[1].metric("Latest Batch", latest_count)
metric_cols[2].metric("Top Technology", top_keyword)
metric_cols[3].metric("Last Update", last_update_text)

if total_events == 0 and keywords_df.empty and repos_df.empty and timeline_df.empty:
    render_empty_state()

left, right = st.columns(2)
with left:
    st.subheader("Top AI Technologies")
    if keywords_df.empty:
        st.write("No keyword data yet.")
    else:
        st.bar_chart(keywords_df.set_index("keyword"))

with right:
    st.subheader("Top AI Repositories")
    if repos_df.empty:
        st.write("No repository data yet.")
    else:
        st.dataframe(repos_df, use_container_width=True, hide_index=True)

st.subheader("AI Activity Over Time")
if timeline_df.empty:
    st.write("No timeline data yet.")
else:
    st.line_chart(timeline_df.set_index("timestamp")[["ai_event_count"]])

st.caption(f"Auto-refresh every {REFRESH_SECONDS}s. Rendered at {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}.")

