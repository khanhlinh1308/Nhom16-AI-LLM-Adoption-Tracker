"""Read GitHub events from Kafka, detect AI/LLM activity, and write metrics to Redis."""

import json
import os
from datetime import datetime, timezone
from typing import Dict

import redis
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    array,
    col,
    collect_list,
    concat,
    count,
    explode,
    hour,
    lit,
    lower,
    regexp_replace,
    to_timestamp,
    when,
    window,
    from_json,
)
from pyspark.sql.types import StringType, StructField, StructType

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "github_events")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
WINDOW_DURATION = os.getenv("WINDOW_DURATION", "1 minute")
SLIDE_DURATION = os.getenv("SLIDE_DURATION", "1 minute")
WATERMARK_DELAY = os.getenv("WATERMARK_DELAY", "2 minutes")
CHECKPOINT_LOCATION = os.getenv("SPARK_CHECKPOINT_LOCATION", "/tmp/spark_checkpoints/ai_tracker")

AI_KEYWORDS = [
    "chatgpt", "llm", "langchain", "rag", "gemini", "claude", "openai",
    "ai agent", "vector database", "gpt", "llama", "mistral", "huggingface",
    "transformer", "embedding", "fine-tune", "fine tuning", "prompt engineering",
    "anthropic", "stable diffusion", "pinecone", "chroma", "weaviate",
    "ollama", "autogen", "crewai", "rlhf", "diffusion model", "machine learning",
    "deep learning", "neural network", "generative ai", "copilot",
]

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("repo_name", StringType(), True),
    StructField("actor_login", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("collected_at", StringType(), True),
    StructField("text_content", StringType(), True),
])


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("AI-LLM-Adoption-Tracker-StreamProcessor")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession):
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    return (
        raw_df
        .selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), EVENT_SCHEMA).alias("data"))
        .select("data.*")
    )


def clean_data(df):
    return (
        df
        .filter(col("event_id").isNotNull() & col("repo_name").isNotNull())
        .withColumn("text_lower", lower(col("text_content")))
        .withColumn("repo_lower", lower(col("repo_name")))
        .withColumn("text_clean", regexp_replace(col("text_lower"), r"[^a-z0-9\s\-]", " "))
        .withColumn("repo_clean", regexp_replace(col("repo_lower"), r"[^a-z0-9\s\-/]", " "))
        .withColumn("event_time", to_timestamp(col("created_at")))
        .filter(col("event_time").isNotNull())
    )


def detect_ai_keywords(df):
    df = df.withColumn("combined_text", concat(col("text_clean"), lit(" "), col("repo_clean")))
    match_exprs = [when(col("combined_text").contains(keyword), lit(keyword)).otherwise(lit(None)) for keyword in AI_KEYWORDS]

    return (
        df
        .withColumn("keyword_array_raw", array(*match_exprs))
        .withColumn("matched_keyword", explode(col("keyword_array_raw")))
        .filter(col("matched_keyword").isNotNull())
        .withColumn("is_ai_related", lit(True))
        .withColumn("hour_utc", hour(col("event_time")))
    )


def build_windowed_aggregations(df):
    return (
        df.withWatermark("event_time", WATERMARK_DELAY)
        .groupBy(window(col("event_time"), WINDOW_DURATION, SLIDE_DURATION), col("matched_keyword"))
        .agg(count("*").alias("event_count"), collect_list("repo_name").alias("repos"))
    )


def write_batch_to_redis(batch_df, batch_id: int) -> None:
    rows = batch_df.collect()
    if not rows:
        return

    client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    pipe = client.pipeline()
    top_keywords: Dict[str, int] = {}
    repo_counter: Dict[str, int] = {}
    total_ai_events = 0

    for row in rows:
        keyword = row["matched_keyword"]
        count_value = int(row["event_count"])
        repos = row["repos"] or []
        top_keywords[keyword] = top_keywords.get(keyword, 0) + count_value
        total_ai_events += count_value
        for repo in repos:
            repo_counter[repo] = repo_counter.get(repo, 0) + 1

    for keyword, count_value in top_keywords.items():
        pipe.zincrby("top_ai_keywords", count_value, keyword)
    for repo, count_value in repo_counter.items():
        pipe.zincrby("top_ai_repos", count_value, repo)

    pipe.incrby("total_ai_events_detected", total_ai_events)
    pipe.lpush("ai_activity_timeline", json.dumps({
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "batch_id": batch_id,
        "ai_event_count": total_ai_events,
        "top_keywords_in_batch": top_keywords,
    }, ensure_ascii=False))
    pipe.ltrim("ai_activity_timeline", 0, 999)
    pipe.execute()
    print(f"[Batch {batch_id}] Wrote {total_ai_events} AI events to Redis.")


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    query = (
        build_windowed_aggregations(detect_ai_keywords(clean_data(read_kafka_stream(spark))))
        .writeStream
        .outputMode("update")
        .foreachBatch(write_batch_to_redis)
        .trigger(processingTime="30 seconds")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()

