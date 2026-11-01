from __future__ import annotations

import json
import logging
import os
import shutil
from datetime import date, timedelta
from pathlib import Path
import  requests
import pandas as pd
import pendulum
import pyarrow as pa
import pyarrow.parquet as pq
from psycopg2.extras import execute_values

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models.param import Param
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger("airflow_log")

EVENTS = Path("/opt/airflow/data/parquet/events")
TMP = EVENTS/"_temp"
CONN_ID = "clickstream_db"
EVENT_SCOPE = ["view","addtocart","transaction"]
SCHEMAS = ["event_id","event_type","event_time","visitor_id",'item_id']

ARROW_SCHEMA = pa.schema([
    ("event_id", pa.string()),
    ("event_type", pa.string()),
    ("event_time", pa.timestamp("us", tz="UTC")),
    ("produced_at", pa.timestamp("us", tz="UTC")),
    ("visitor_id", pa.int64()),
    ("item_id", pa.int64()),
    ("transaction_id", pa.int64()),
    ("schema_version", pa.int32()),
    ("source", pa.string()),
])

def notify_fail(context):
    msg = (f":rotating_light: `{context['task_instance'].dag_id}.{context['task_instance'].task_id}` 실패 "
           f"(target_date={context['params'].get('target_date') or context['data_interval_start'].date()}, "
           f"try={context['task_instance'].try_number}) — {context.get('exception')}")
    log.error("ALERT %s", msg)

    url = os.environ.get("DISCORD_WEBHOOK_URL")
    if url:
        requests.post(url, json={'content':msg}, timeout=10)

def p_dir(date):
    return EVENTS/f"dt={date}"

def combine_day(date):
    return pq.read_table( p_dir(date), schema=ARROW_SCHEMA).to_pandas()

default_args = {
    "owner": "clickstream",
    "retries":3,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback":notify_fail,
    "retry_exponential_backoff": True,
}

@dag(
    dag_id="daily_events",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2015, 5, 3, tz="UTC"),
    catchup=False,
    max_active_runs=1,  # 같은 dt 폴더를 두 실행이 동시에 교체하지 않도록
    default_args=default_args,
    params={"target_date": Param(None, type=["null", "string"], format="date",
                                 description="처리할 날짜 (YYYY-MM-DD). 비우면 data_interval_start 의 날짜")},
    tags=["clickstream"],
)

def main():

    @task(retries=0)     # 파티션 폴더가 있는지 확인하는것
    def check_partition(**context):
        if context["params"].get("target_date"):
            date1 = context["params"].get("target_date")
        else:
            date1 = context["data_interval_start"].date().isoformat()

        p = p_dir(date1)

        if not p.exists() or not any(p.glob("*.parquet")):
            raise AirflowSkipException(f"{p} 에 데이터가 없음")
        log.info("target_date=%s files=%d", date1, len(list(p.glob("*.parquet"))))
        return date1

    @task
    def make_parquet(date):
        src = p_dir(date)
        parquet_cnt = len(list(src.glob("*.parquet")))
        table = pq.read_table(src,schema=ARROW_SCHEMA)
        rows_cnt = table.num_rows

        df = (
            table.to_pandas()
            .drop_duplicates("event_id")
            .sort_values(["event_time", "event_id"], ignore_index=True)
        )
        tmp = TMP/src.name
        shutil.rmtree(tmp,ignore_errors=True)
        tmp.mkdir(parents=True)
        pq.write_table(pa.Table.from_pandas(df, schema=ARROW_SCHEMA, preserve_index=False),
                       tmp / "combined.parquet", compression="snappy")

        shutil.rmtree(src,ignore_errors=True)
        tmp.rename(src)

        result = {"parquet_cnt": parquet_cnt, "files_after": 1,
                  "rows_cnt": rows_cnt, "rows_after": len(df), "duplicates_removed": rows_cnt - len(df)}
        log.info("compact %s %s", date, result)
        return result

    @task
    def daily_agg(date):
        df = combine_day(date)
        counts = (pd.crosstab(df["item_id"], df["event_type"])
                  .reindex(columns=list(EVENT_SCOPE), fill_value=0))

        visitors = df.groupby("item_id")["visitor_id"].nunique()
        agg = counts.join(visitors.rename("unique_visitors")).reset_index()
        rows = [(date, int(r.item_id), int(r.view), int(r.addtocart), int(r.transaction), int(r.unique_visitors))
                for r in agg.itertuples(index=False)]

        conn = PostgresHook(postgres_conn_id=CONN_ID).get_conn()
        try:
            with conn, conn.cursor() as cur:
                cur.execute("DELETE FROM item_stats_daily WHERE dt = %s", (date,))
                execute_values(cur, """
                    INSERT INTO item_stats_daily
                        (dt, item_id, view_cnt, addtocart_cnt, transaction_cnt, unique_visitors)
                    VALUES %s""", rows, page_size=5000)
        finally:
            conn.close()
        log.info("daily_agg %s items=%d events=%d", date, len(rows), len(df))
        return len(rows)

    @task(retries=0)
    def data_check(date):
        df = combine_day(date)
        with PostgresHook(postgres_conn_id=CONN_ID).get_conn() as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT coalesce(sum(view_cnt + addtocart_cnt + transaction_cnt), 0)
                FROM item_stats_daily WHERE dt = %s""", (date,))
            agg_total = int(cur.fetchone()[0])

        checks = {
            "row_count": len(df),
            "null_required": int(df[SCHEMAS].isna().sum().sum()),
            "duplicate_event_id": int(df["event_id"].duplicated().sum()),
            "bad_event_type": int((~df["event_type"].isin(EVENT_SCOPE)).sum()),
            "transaction_without_id": int(((df["event_type"] == "transaction") & df["transaction_id"].isna()).sum()),
            "wrong_date": int((df["event_time"].dt.tz_convert("UTC").dt.date.astype(str) != date).sum()),
            "agg_total": agg_total,
        }
        log.info("quality %s %s", date, checks)

        failed = [k for k in ("null_required", "duplicate_event_id", "bad_event_type",
                              "transaction_without_id", "wrong_date") if checks[k] > 0]
        if checks["row_count"] == 0:
            failed.append("row_count")
        if agg_total != checks["row_count"]:
            failed.append("agg_total != row_count")
        if failed:
            raise AirflowFailException(f"품질 검사 실패 {date}: {failed} {checks}")
        return checks

    @task
    def publish(d: str) -> dict:
        """검사를 통과한 날짜만 대시보드 테이블(daily_summary)에 올린다. PK(dt) 업서트."""
        df = combine_day(d)
        n = df["event_type"].value_counts()
        views, carts, txs = (int(n.get(t, 0)) for t in EVENT_SCOPE)
        row = {
            "dt": d,
            "events": len(df),
            "view_cnt": views,
            "addtocart_cnt": carts,
            "transaction_cnt": txs,
            "unique_visitors": int(df["visitor_id"].nunique()),
            "unique_items": int(df["item_id"].nunique()),
            "cart_rate": round(carts / views, 6) if views else None,
            "purchase_rate": round(txs / views, 6) if views else None,
        }
        with PostgresHook(postgres_conn_id=CONN_ID).get_conn() as conn, conn.cursor() as cur:
            cur.execute("""
                    INSERT INTO daily_summary
                        (dt, events, view_cnt, addtocart_cnt, transaction_cnt,
                         unique_visitors, unique_items, cart_rate, purchase_rate, updated_at)
                    VALUES (%(dt)s, %(events)s, %(view_cnt)s, %(addtocart_cnt)s, %(transaction_cnt)s,
                            %(unique_visitors)s, %(unique_items)s, %(cart_rate)s, %(purchase_rate)s, now())
                    ON CONFLICT (dt) DO UPDATE SET
                        events = EXCLUDED.events, view_cnt = EXCLUDED.view_cnt,
                        addtocart_cnt = EXCLUDED.addtocart_cnt, transaction_cnt = EXCLUDED.transaction_cnt,
                        unique_visitors = EXCLUDED.unique_visitors, unique_items = EXCLUDED.unique_items,
                        cart_rate = EXCLUDED.cart_rate, purchase_rate = EXCLUDED.purchase_rate,
                        updated_at = now()""", row)
        log.info("publish %s", row)
        return row

    d = check_partition()
    compacted = make_parquet(d)
    agg = daily_agg(d)
    checked = data_check(d)
    published = publish(d)
    compacted >> agg >> checked >> published

main()



