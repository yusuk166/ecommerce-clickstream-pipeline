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
import time

import psycopg2
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (IntegerType, LongType, StringType, StructField,
                               StructType)

log = logging.getLogger("preprocess")

# ----------------------------------------------------------------------
# 설정
# ----------------------------------------------------------------------
PG = {"host": "localhost", "port": 5432, "dbname": "clickstream", "user": "clickstream", "password": "clickstream"}
PG_JDBC_URL = f"jdbc:postgresql://{PG['host']}:{PG['port']}/{PG['dbname']}?stringtype=unspecified"  # 문자열 → uuid 자동 캐스팅
EVENT_TYPES = ("view", "addtocart", "transaction")

# 생성기(producer.py)가 보내는 메시지 모양. 이 스키마로 못 읽으면 DLQ 행
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
            # Kafka 소스/싱크 + PostgreSQL JDBC 드라이버 (Maven 에서 자동 다운로드, 최초 1회)
            .config("spark.jars.packages",
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.postgresql:postgresql:42.7.4")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.ui.enabled", "false")
            .getOrCreate())


# ----------------------------------------------------------------------
# 1) 파싱 + 검증: raw Kafka 행 → (정상 DataFrame, DLQ DataFrame)
# ----------------------------------------------------------------------
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

    # 왜 실패했는지 남겨야 DLQ 를 보고 고칠 수 있다
    # from_json 은 JSON 이 아니어도 null 구조체가 아니라 "모든 필드가 null 인 구조체" 를 줄 수 있어서 둘 다 invalid_json 으로 본다
    all_null = (F.col("m").isNull()
                | (F.col("m.event_id").isNull() & F.col("m.event_type").isNull() & F.col("m.event_time").isNull()
                   & F.col("m.payload").isNull()))
    
    reason = (F.when(all_null, "invalid_json")
               .when(F.col("event_id").isNull(), "missing_event_id")
               .when(~F.col("event_type").isin(EVENT_TYPES), "bad_event_type")
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


# ----------------------------------------------------------------------
# 2) PostgreSQL: staging → events (ON CONFLICT) → item_stats_1m 재계산
# ----------------------------------------------------------------------
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



# 이번 배치에 등장한 1분 윈도우만 events 에서 다시 세서 덮어쓴다.
# 배치마다 "더하기" 가 아니라 "다시 계산" 이라 같은 입력을 두 번 처리해도 값이 안 불어난다.
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


# PostgreSQL 은 조회용이라 최근 N일만 둔다. 전체 이력은 Parquet 이 가진다.
# 기준은 now() 가 아니라 "저장된 가장 최근 이벤트 시각" — 2015년 로그를 리플레이하는 중이라 now() 기준이면 전부 지워진다.
# (운영에서 실시간 수집이면 latest ≈ now() 라 결과는 같다)
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


def load_to_postgres(good: DataFrame, retention_days: int) -> dict:
    # Spark → staging (JDBC). overwrite + truncate 라 테이블 정의는 유지되고 내용만 바뀐다
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
        windows_updated = cur.rowcount
        # 보관 기간 밖으로 밀려난 오래된 행 삭제 (events · item_stats_1m). Parquet 은 건드리지 않는다
        cur.execute(DELETE_OLD, {"days": retention_days})
        deleted_events, deleted_stats, latest = cur.fetchone()
        conn.commit()
    return {"staged": staged, "inserted": inserted, "skipped_existing": staged - inserted,
            "stats_rows_upserted": windows_updated,
            "deleted_events": deleted_events, "deleted_stats": deleted_stats, "latest_event_time": latest}


# ----------------------------------------------------------------------
# 3) micro-batch 처리 본체
# ----------------------------------------------------------------------
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

        # --- DLQ: 파싱/검증 실패 → user-events-dlq 토픽으로 (원문 + 이유)
        n_bad = bad.count()
        if n_bad:
            (bad.select(F.col("kafka_key").alias("key"),
                        F.to_json(F.struct("raw_value", "partition", "offset", "dlq_reason",
                                           F.lit("preprocess.py").alias("rejected_by"))).alias("value"))
                .write.format("kafka")
                .option("kafka.bootstrap.servers", args.bootstrap)
                .option("topic", args.dlq_topic)
                .save())

        # --- 중복 제거: 같은 배치 안에 같은 event_id 가 두 번 있으면 하나만
        n_good = good.count()
        dedup = good.dropDuplicates(["event_id"]).withColumn("dt", F.to_date("event_time")).cache()
        n_dedup = dedup.count()

        # --- Parquet: 원본 보관 (날짜 파티션, append). 재처리 입력이자 영구 보관본
        (dedup.write.mode("append").partitionBy("dt").parquet(args.parquet_dir))

        # --- PostgreSQL: 조회용
        pg = load_to_postgres(dedup.drop("dt"), args.pg_retention_days)

        log.info("batch=%d kafka=%d parsed_ok=%d dlq=%d dedup=%d(-%d) | pg inserted=%d skipped_existing=%d stats_upsert=%d "
                 "| retention(%dd, latest=%s) deleted events=%d stats=%d | parquet=%s | %.1fs",
                 batch_id, n_raw, n_good, n_bad, n_dedup, n_good - n_dedup,
                 pg["inserted"], pg["skipped_existing"], pg["stats_rows_upserted"],
                 args.pg_retention_days, pg["latest_event_time"], pg["deleted_events"], pg["deleted_stats"],
                 args.parquet_dir, time.time() - t0)
        dedup.unpersist(); raw.unpersist()
    return handle


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="user-events")
    ap.add_argument("--dlq-topic", default="user-events-dlq")
    ap.add_argument("--parquet-dir", default="data/parquet/events")
    ap.add_argument("--checkpoint", default="checkpoint/preprocess")
    ap.add_argument("--pg-retention-days", type=int, default=30,
                    help="PostgreSQL 에 남길 기간(일). 가장 최근 이벤트 시각 기준. Parquet 은 전부 보관")
    ap.add_argument("--continuous", action="store_true", help="1분 트리거로 계속 실행 (기본: 쌓인 것만 처리하고 종료)")
    ap.add_argument("--reset", action="store_true", help="체크포인트를 지우고 토픽 처음부터 다시 처리")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)
    # Spark 워커도 드라이버와 같은 파이썬을 쓰게 (Windows 에서 python3 를 못 찾는 문제 방지)
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    if args.reset and os.path.exists(args.checkpoint):
        shutil.rmtree(args.checkpoint)
        log.info("checkpoint 삭제: %s", args.checkpoint)

    spark = build_spark("clickstream-preprocess")
    spark.sparkContext.setLogLevel("WARN")

    stream = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", args.bootstrap)
              .option("subscribe", args.topic)
              .option("startingOffsets", "earliest")   # 체크포인트가 있으면 그쪽이 우선
              .option("failOnDataLoss", "false")
              .load())

    writer = (stream.writeStream
              .foreachBatch(make_batch_handler(args))
              .option("checkpointLocation", args.checkpoint))
    if args.continuous:
        query = writer.trigger(processingTime="1 minute").start()
        log.info("continuous 모드: 1분마다 micro-batch. Ctrl+C 로 종료")
        query.awaitTermination()
    else:
        query = writer.trigger(availableNow=True).start()
        query.awaitTermination()
        log.info("DONE 쌓인 메시지 처리 완료")
    spark.stop()


if __name__ == "__main__":
    main()
