import json
import os
import threading
import time

import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

POSTGRES = {"host": "localhost", "port": 5432, "dbname": "clickstream", "user": "clickstream", "password": "clickstream"}

JDBC = {"url": "jdbc:postgresql://localhost:5432/clickstream?stringtype=unspecified",
        "dbtable": "events_staging", "user": "clickstream", "password": "clickstream",
        "driver": "org.postgresql.Driver", "truncate": "true"}


MSG_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_time", StringType()),
    StructField("produced_at", StringType()),
    StructField("schema_version", IntegerType()),
    StructField("source", StringType()),
    StructField("payload",StructType([
        StructField("visitor_id",LongType()),
        StructField("item_id",LongType()),
        StructField("transaction_id",LongType()),
    ]))
])

UPSERT_EVENTS = """
INSERT INTO events (event_id, event_type, event_time, produced_at, visitor_id, item_id,
                    transaction_id, schema_version, source)
SELECT event_id, event_type, event_time, produced_at, visitor_id, item_id,
       transaction_id, schema_version, source
FROM events_staging
ON CONFLICT (event_id) DO NOTHING
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
    JOIN touched t ON e.event_time >= t.window_start
    and e.event_time <t.window_start + interval '1 minute'
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






def parse(raw):
    return (
        raw.select("key", "value", F.from_json(F.col("value").cast("string"), MSG_SCHEMA).alias("json"))
        .select("key","value","json.*", "json.payload.*").drop("payload")
        .withColumn("event_time",F.to_timestamp("event_time"))
        .withColumn("produced_at",F.to_timestamp("produced_at"))
    )



def is_valid():
    return F.coalesce(
        F.col("event_id").isNotNull()& F.col("event_type").isin("view","transaction","addtocart")
        &F.col("event_time").isNotNull()
        &F.col("produced_at").isNotNull()
        &F.col("visitor_id").isNotNull()
        &F.col("item_id").isNotNull()
        &(((F.col("event_type")=="transaction") & F.col("transaction_id").isNotNull()) |
          F.col("event_type").isin("view","addtocart"))

    ,F.lit(False)
    )


def save(df):
    df.write.format("jdbc").options(**JDBC).mode("overwrite").save()
    with psycopg2.connect(**POSTGRES) as conn, conn.cursor() as cur:
        cur.execute(UPSERT_EVENTS)
        cur.execute(LATENCY_SQL)
        p50,p95,_ = cur.fetchone()
        cur.execute(RECOMPUTE_STATS)
        cur.execute(DELETE_OLD, {"days":30})
    return p50, p95

def save_retry(df,retries=6):
    for i in range(retries):
        try:
            return save(df)
        except Exception as e:
            if i+1==retries:
                raise
            delay = min(5* 2**i,30)
            print(f"DB 저장 실패 {i+1}/{retries}, {delay}초 뒤 다시 시도: {e}")
            time.sleep(delay)


def handle(raw, batch_id):
    #"data/parquet/events" parquet 저장위치
    t0 = time.time()
    df = parse(raw).cache()
    bad = df.filter(~is_valid())
    good = df.filter(is_valid()).drop("key","value").dropDuplicates(["event_id"]).cache()

    (bad.select("key","value").write.format("kafka")
     .option("kafka.bootstrap.servers", "localhost:9092")
     .option("topic", "user-events-dlq")
     .save()
     )
    n = good.count()
    t1 = time.time()

    good.withColumn("dt",F.to_date("event_time")).write.mode("append").partitionBy("dt").parquet("data/parquet/loadtest")
    t2 = time.time()
    p50,p95=save_retry(good)     #post gres 에 저장 시간
    t3=time.time()
    append_jsonl("data/metrics/batches.jsonl", {
        "ts": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "spark_s": round(t1 - t0, 1),
        "parquet_s": round(t2 - t1, 1),
        "pg_s": round(t3 - t2, 1),
        "batch": batch_id,
        "lat_p50": p50,
        "lat_p95": p95})
    print(f"batch={batch_id} saved={good.count()}")

    good.unpersist()
    df.unpersist()


#입력, 처리 속도, 배치 처리시간 ,lag , 지연 p50. p95

def log_progress(query):
    seen = None
    while query.isActive:
        time.sleep(1)
        res = query.lastProgress
        if res is None or res["batchId"]==seen:
            continue
        seen = res["batchId"]
        s = res["sources"][0]
        if not s.get("latestOffset") or not s.get("endOffset"):
            continue
        lag = sum(s["latestOffset"]["user-events"].values())-sum(s["endOffset"]["user-events"].values())
        append_jsonl("data/metrics/progress.jsonl",{
            "batch":res["batchId"],
            "input_rows": res["numInputRows"],
            "input_rps":res["inputRowsPerSecond"],
            "processed_rps":res["processedRowsPerSecond"],
            "batch_ms":res["durationMs"]["triggerExecution"]   ,
            "lag":lag,
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S")
        })

def append_jsonl(path, json_data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path,"a", encoding="utf-8")as f:
        f.write(json.dumps(json_data)+"\n")


def main():
    spark = (SparkSession.builder.appName("preprocess")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions","8")
        .config("spark.sql.session.timeZone","UTC")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.postgresql:postgresql:42.7.4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
             )

    stream = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", "localhost:9092")
              .option("subscribe", "user-events")
              .option("startingOffsets","earliest")
              .option("maxOffsetsPerTrigger",1000000)
              .option("failOnDataLoss", "false")
              .load())



    query=(stream.writeStream.foreachBatch(handle).
    option("checkpointLocation","checkpoint/loadtest")
    .trigger(processingTime="10 seconds")
    .start())

    threading.Thread(target=log_progress, args=(query,), daemon=True).start()
    query.awaitTermination()


if __name__ == "__main__":
    main()
