"""Kafka(user-events) → Spark 전처리 → Parquet(원본 보관) + PostgreSQL(조회용) 저장.

흐름 (micro-batch 하나 기준, foreachBatch 안에서):
  Kafka 읽기 → JSON 파싱 → 검증(필수 필드·타입·값 범위) ─┬─ 실패 → user-events-dlq 로 격리
                                                        └─ 성공 → event_id 기준 중복 제거
                                                                 → Parquet append (data/parquet/events/dt=YYYY-MM-DD)
                                                                 → PostgreSQL events_staging (JDBC overwrite)
                                                                 → events 로 INSERT ... ON CONFLICT DO NOTHING (멱등)
                                                                 → 이번 배치가 건드린 1분 윈도우만 item_stats_1m 재계산 upsert
                                                                 → 가장 최근 이벤트 기준 N일(기본 30) 지난 행은 PostgreSQL 에서 삭제

저장소 역할:
  - Parquet     : 전체 이력. 지우지 않는다. 재처리 · 과거 분석용
  - PostgreSQL  : 최근 N일만. 조회(API)용. --pg-retention-days 로 조절

재실행 동작:
  - 체크포인트(checkpoint/preprocess)에 Kafka offset 이 기록돼서, 다시 실행하면 지난번 이후 메시지만 읽는다.
  - 체크포인트를 지우고 처음부터 다시 돌려도 events 는 PK(event_id) 로 중복이 안 생기고,
    item_stats_1m 은 events 에서 다시 계산하므로 같은 값이 나온다. Parquet 만 append 라 중복 파일이 생긴다 (README 참고).

실행:
  python preprocess.py                 # 지금까지 쌓인 메시지를 전부 처리하고 종료 (trigger availableNow)
  python preprocess.py --continuous    # 1분마다 micro-batch 를 계속 돌림 (Ctrl+C 로 종료)
  python preprocess.py --reset         # 체크포인트 삭제 후 처음부터 (재처리 테스트용)
"""
import argparse
import json
import logging
import os
import shutil
import sys
import threading
import time

import psycopg2
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (IntegerType, LongType, StringType, StructField,
                               StructType)

log = logging.getLogger("preprocess")


PG = {"host": "localhost", "port": 5432, "dbname": "clickstream", "user": "clickstream", "password": "clickstream"}
PG_JDBC_URL = f"jdbc:postgresql://{PG['host']}:{PG['port']}/{PG['dbname']}?stringtype=unspecified"  # 문자열 → uuid 자동 캐스팅
EVENT_TYPES = ("view", "addtocart", "transaction")


MESSAGE_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_time", StringType()),
    StructField("produced_at", StringType()),
    StructField("schema_version", IntegerType()),
    StructField("source", StringType()),
    StructField("payload", StructType([
        StructField("visitor_id", LongType()),
        StructField("item_id", LongType()),
        StructField("transaction_id", LongType()),
    ])),
])


def build_spark(app: str) -> SparkSession:
    return (SparkSession.builder
            .appName(app)
            .master("local[*]")

            .config("spark.jars.packages",
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.postgresql:postgresql:42.7.4")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.ui.enabled", "false")
            .getOrCreate())



def parse_and_validate(raw: DataFrame):
    parsed = (raw
              .select(F.col("key").cast("string").alias("kafka_key"),
                      F.col("value").cast("string").alias("raw_value"),
                      "partition", "offset")
              .withColumn("m", F.from_json("raw_value", MESSAGE_SCHEMA)))  # 깨진 JSON → m 이 null

    flat = (parsed
            .withColumn("event_id", F.col("m.event_id"))
            .withColumn("event_type", F.col("m.event_type"))
            # 문자열 ISO 시각 → timestamp. 'Z' 가 붙은 UTC 라 to_timestamp 가 바로 읽는다. 못 읽으면 null
            .withColumn("event_time", F.to_timestamp("m.event_time"))
            .withColumn("produced_at", F.to_timestamp("m.produced_at"))
            .withColumn("visitor_id", F.col("m.payload.visitor_id"))
            .withColumn("item_id", F.col("m.payload.item_id"))
            .withColumn("transaction_id", F.col("m.payload.transaction_id"))
            .withColumn("schema_version", F.col("m.schema_version"))
            .withColumn("source", F.col("m.source")))


    all_null = (F.col("m").isNull()
                | (F.col("m.event_id").isNull() & F.col("m.event_type").isNull() & F.col("m.event_time").isNull()
                   & F.col("m.payload").isNull()))
    
    reason = (F.when(all_null, "invalid_json")
               .when(F.col("event_id").isNull(), "missing_event_id")
               .when(~F.col("event_type").isin(*EVENT_TYPES), "bad_event_type")
               .when(F.col("event_time").isNull(), "bad_event_time")
               .when(F.col("visitor_id").isNull() | F.col("item_id").isNull(), "missing_ids")
               .when((F.col("event_type") == "transaction") & F.col("transaction_id").isNull(), "transaction_without_id")
               .otherwise(F.lit(None)))
    flat = flat.withColumn("dlq_reason", reason)

    good = (flat.filter(F.col("dlq_reason").isNull())
            .select("event_id", "event_type", "event_time", "produced_at",
                    "visitor_id", "item_id", "transaction_id", "schema_version", "source"))
    bad = flat.filter(F.col("dlq_reason").isNotNull()).select("kafka_key", "raw_value", "partition", "offset", "dlq_reason")
    return good, bad



UPSERT_EVENTS = """
WITH ins AS (
    INSERT INTO events (event_id, event_type, event_time, produced_at, visitor_id, item_id,
                        transaction_id, schema_version, source)
    SELECT event_id, event_type, event_time, produced_at, visitor_id, item_id,
           transaction_id, schema_version, source
    FROM events_staging
    ON CONFLICT (event_id) DO NOTHING      -- 이미 있는 이벤트는 건너뜀 (원본 이벤트는 불변)
    RETURNING 1
)
SELECT count(*) FROM ins;
"""




RECOMPUTE_STATS = """
WITH touched AS (
    SELECT DISTINCT date_trunc('minute', event_time) AS window_start FROM events_staging
),
agg AS (
    SELECT date_trunc('minute', e.event_time) AS window_start,
           e.item_id,
           count(*) FILTER (WHERE e.event_type = 'view')        AS view_cnt,
           count(*) FILTER (WHERE e.event_type = 'addtocart')   AS addtocart_cnt,
           count(*) FILTER (WHERE e.event_type = 'transaction') AS transaction_cnt,
           count(DISTINCT e.visitor_id)                          AS unique_visitors
    FROM events e
    JOIN touched t ON date_trunc('minute', e.event_time) = t.window_start
    GROUP BY 1, 2
)
INSERT INTO item_stats_1m (window_start, item_id, view_cnt, addtocart_cnt, transaction_cnt, unique_visitors, updated_at)
SELECT window_start, item_id, view_cnt, addtocart_cnt, transaction_cnt, unique_visitors, now() FROM agg
ON CONFLICT (window_start, item_id) DO UPDATE SET
    view_cnt = EXCLUDED.view_cnt,
    addtocart_cnt = EXCLUDED.addtocart_cnt,
    transaction_cnt = EXCLUDED.transaction_cnt,
    unique_visitors = EXCLUDED.unique_visitors,
    updated_at = now();
"""



DELETE_OLD = """
WITH latest AS (
    SELECT max(event_time) AS t FROM events
),
d1 AS (
    DELETE FROM events
    WHERE event_time < (SELECT t FROM latest) - make_interval(days => %(days)s)
    RETURNING 1
),
d2 AS (
    DELETE FROM item_stats_1m
    WHERE window_start < date_trunc('minute', (SELECT t FROM latest) - make_interval(days => %(days)s))
    RETURNING 1
)
SELECT (SELECT count(*) FROM d1), (SELECT count(*) FROM d2), (SELECT t FROM latest);
"""


LATENCY_SQL = """
SELECT percentile_cont(0.5)  WITHIN GROUP (ORDER BY extract(epoch FROM e.ingested_at - e.produced_at)),
       percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM e.ingested_at - e.produced_at)),
       max(extract(epoch FROM e.ingested_at - e.produced_at))
FROM events e JOIN events_staging s USING (event_id)
WHERE e.ingested_at = now();
"""


def load_once(good, retention_days):
    (good.write.format("jdbc")
         .option("url", PG_JDBC_URL)
         .option("dbtable", "events_staging")
         .option("user", PG["user"]).option("password", PG["password"])
         .option("driver", "org.postgresql.Driver")
         .option("truncate", "true")
         .mode("overwrite")
         .save())
    with psycopg2.connect(**PG) as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM events_staging")
        staged = cur.fetchone()[0]
        cur.execute(UPSERT_EVENTS)
        inserted = cur.fetchone()[0]
        cur.execute(RECOMPUTE_STATS)
        cur.execute(LATENCY_SQL)
        p50, p95, p_max = cur.fetchone()
        cur.execute(DELETE_OLD, {"days": retention_days})
        deleted = cur.fetchone()[0]
        conn.commit()
    return {"inserted": inserted, "skipped_existing": staged - inserted, "deleted_events": deleted,
            "lat_p50": p50, "lat_p95": p95, "lat_max": p_max}


def load_to_postgres(good, retention_days, retries=6):
    for attempt in range(1, retries + 1):
        try:
            return load_once(good, retention_days)
        except Exception as e:
            if attempt == retries:
                raise
            delay = min(5 * 2 ** (attempt - 1), 30)
            log.warning("pg 실패 %d/%d, %ds 뒤 재시도: %s", attempt, retries, delay, str(e).splitlines()[0][:120])
            time.sleep(delay)


def make_batch_handler(args):
    def handle(raw: DataFrame, batch_id: int):
        t0 = time.time()
        raw = raw.cache()
        n_raw = raw.count()

        if n_raw == 0:
            log.info("batch=%d 새 메시지 없음", batch_id)
            raw.unpersist()
            return

        good, bad = parse_and_validate(raw)

        n_bad = bad.count()
        if n_bad:
            (bad.select(F.col("kafka_key").alias("key"),
                        F.to_json(F.struct("raw_value", "partition", "offset", "dlq_reason",
                                           F.lit("preprocess.py").alias("rejected_by"))).alias("value"))
                .write.format("kafka")
                .option("kafka.bootstrap.servers", args.bootstrap)
                .option("topic", args.dlq_topic)
                .save())

        n_good = good.count()
        dedup = good.dropDuplicates(["event_id"]).withColumn("dt", F.to_date("event_time")).cache()
        n_dedup = dedup.count()
        t1 = time.time()

        dedup.write.mode("append").partitionBy("dt").parquet(args.parquet_dir)
        t2 = time.time()

        pg = load_to_postgres(dedup.drop("dt"), args.pg_retention_days)
        t3 = time.time()

        log.info("batch=%d in=%d dlq=%d dup=%d | pg new=%d skip=%d old_deleted=%d | p95=%.1fs | spark %.1fs parquet %.1fs pg %.1fs",
                 batch_id, n_raw, n_bad, n_good - n_dedup, pg["inserted"], pg["skipped_existing"], pg["deleted_events"],
                 pg["lat_p95"] or 0, t1 - t0, t2 - t1, t3 - t2)
        append_jsonl(f"{args.metrics_dir}/batches.jsonl", {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S"), "batch": batch_id, "kafka": n_raw, "dlq": n_bad, "inserted": pg["inserted"],
            "batch_s": round(t3 - t0, 1), "spark_s": round(t1 - t0, 1), "parquet_s": round(t2 - t1, 1), "pg_s": round(t3 - t2, 1),
            "lat_p50": pg["lat_p50"], "lat_p95": pg["lat_p95"], "lat_max": pg["lat_max"]})
        dedup.unpersist(); raw.unpersist()
    return handle


def append_jsonl(path, row):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")


def progress_logger(query, metrics_dir):
    seen = None
    while query.isActive:
        p = query.lastProgress
        if p and p["batchId"] != seen and p["numInputRows"]:
            seen = p["batchId"]
            row = {"ts": time.strftime("%Y-%m-%dT%H:%M:%S"), "batch": seen, "input_rows": p["numInputRows"],
                   "input_rps": round(p["inputRowsPerSecond"], 1), "processed_rps": round(p["processedRowsPerSecond"], 1),
                   "batch_ms": p["durationMs"]["triggerExecution"]}
            log.info("PROGRESS %s", json.dumps(row))
            append_jsonl(f"{metrics_dir}/progress.jsonl", row)
        time.sleep(1)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="user-events")
    ap.add_argument("--dlq-topic", default="user-events-dlq")
    ap.add_argument("--parquet-dir", default="data/parquet/events")
    ap.add_argument("--checkpoint", default="checkpoint/preprocess")
    ap.add_argument("--pg-retention-days", type=int, default=30)
    ap.add_argument("--continuous", action="store_true")
    ap.add_argument("--trigger-seconds", type=int, default=60)
    ap.add_argument("--max-offsets-per-trigger", type=int)
    ap.add_argument("--metrics-dir", default="data/metrics")
    ap.add_argument("--starting-offsets", default="earliest", choices=["earliest", "latest"])
    ap.add_argument("--reset", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    if args.reset and os.path.exists(args.checkpoint):
        shutil.rmtree(args.checkpoint)
        log.info("checkpoint 삭제: %s", args.checkpoint)

    spark = build_spark("clickstream-preprocess")
    spark.sparkContext.setLogLevel("WARN")

    reader = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", args.bootstrap)
              .option("subscribe", args.topic)
              .option("startingOffsets", args.starting_offsets)
              .option("failOnDataLoss", "false"))
    if args.max_offsets_per_trigger:
        reader = reader.option("maxOffsetsPerTrigger", args.max_offsets_per_trigger)

    trigger = {"processingTime": f"{args.trigger_seconds} seconds"} if args.continuous else {"availableNow": True}
    query = (reader.load().writeStream
             .foreachBatch(make_batch_handler(args))
             .option("checkpointLocation", args.checkpoint)
             .trigger(**trigger)
             .start())
    log.info("started %s", trigger)
    threading.Thread(target=progress_logger, args=(query, args.metrics_dir), daemon=True).start()
    query.awaitTermination()
    spark.stop()


if __name__ == "__main__":
    main()
