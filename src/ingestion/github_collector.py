"""Collect public GitHub events and publish normalized records to Kafka."""

import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import List, Set

import requests
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "github_events")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "30"))
GITHUB_EVENTS_URL = "https://api.github.com/events"

RELEVANT_EVENT_TYPES = {
    "PushEvent",
    "CreateEvent",
    "PullRequestEvent",
    "WatchEvent",
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("github_collector")


def create_kafka_producer(max_retries: int = 30, retry_delay: int = 5) -> KafkaProducer:
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                retries=5,
                linger_ms=200,
            )
            logger.info("Connected to Kafka broker %s", KAFKA_BROKER)
            return producer
        except KafkaError as exc:
            logger.warning(
                "Kafka is not ready yet (%d/%d): %s. Retrying in %ds...",
                attempt,
                max_retries,
                exc,
                retry_delay,
            )
            time.sleep(retry_delay)
    raise RuntimeError("Could not connect to Kafka after multiple retries.")


def fetch_github_events() -> List[dict]:
    headers = {"Accept": "application/vnd.github+json"}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"

    try:
        response = requests.get(GITHUB_EVENTS_URL, headers=headers, timeout=15)
        response.raise_for_status()
        remaining = response.headers.get("X-RateLimit-Remaining")
        if remaining is not None:
            logger.info("GitHub API rate limit remaining: %s", remaining)
        return response.json()
    except requests.RequestException as exc:
        logger.error("GitHub API request failed: %s", exc)
        return []


def extract_text_content(event: dict) -> str:
    parts: List[str] = []
    payload = event.get("payload", {}) or {}
    repo_name = event.get("repo", {}).get("name", "")
    event_type = event.get("type")

    if repo_name:
        parts.append(repo_name)

    if event_type == "PushEvent":
        parts.extend(commit.get("message", "") for commit in payload.get("commits", []))
    elif event_type == "CreateEvent":
        parts.append(payload.get("description", "") or "")
    elif event_type == "PullRequestEvent":
        pull_request = payload.get("pull_request", {}) or {}
        parts.append(pull_request.get("title", "") or "")
        parts.append(pull_request.get("body", "") or "")

    return " ".join(part for part in parts if part)


def normalize_event(event: dict) -> dict:
    return {
        "event_id": event.get("id"),
        "event_type": event.get("type"),
        "repo_name": event.get("repo", {}).get("name", ""),
        "actor_login": event.get("actor", {}).get("login", ""),
        "created_at": event.get("created_at"),
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "text_content": extract_text_content(event),
    }


def run_collector() -> None:
    producer = create_kafka_producer()
    seen_event_ids: Set[str] = set()
    logger.info("Collecting GitHub events every %ds into topic %s", POLL_INTERVAL_SECONDS, KAFKA_TOPIC)

    while True:
        new_count = 0
        for event in fetch_github_events():
            event_id = event.get("id")
            event_type = event.get("type")
            if not event_id or event_id in seen_event_ids or event_type not in RELEVANT_EVENT_TYPES:
                continue

            record = normalize_event(event)
            try:
                producer.send(KAFKA_TOPIC, key=record["repo_name"], value=record)
                seen_event_ids.add(event_id)
                new_count += 1
            except KafkaError as exc:
                logger.error("Could not publish message to Kafka: %s", exc)

        producer.flush()
        logger.info("Published %d new events to Kafka; cached IDs: %d", new_count, len(seen_event_ids))

        if len(seen_event_ids) > 20000:
            seen_event_ids = set(list(seen_event_ids)[-10000:])

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    run_collector()

