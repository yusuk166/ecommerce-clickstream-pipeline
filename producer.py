"""events.csv 리플레이 생성기 → Kafka producer.

흐름:  CSV 읽기(시간순) → JSON 직렬화(공통 껍데기 + payload) → produce(key, value) → 콜백으로 성공/실패 확인

속도 제어:
  --speed N   원본 이벤트 간 시간 간격을 N배속으로 재생 (1 = 원본 속도, 0 = 간격 무시하고 최대 속도)
  --rate N    초당 N건으로 고정 전송 (지정 시 --speed 무시, 로드 테스트용)

예)
  python producer.py --limit 1000 --speed 0          # 1000건 최대 속도로
  python producer.py --speed 100                     # 원본 100배속 리플레이
  python producer.py --rate 500 --limit 60000        # 초당 500건 고정, 6만 건
"""
import argparse
import json
import logging
import math
import signal
import sys
import time
import uuid
from datetime import datetime, timezone

import pandas as pd
from confluent_kafka import KafkaError, KafkaException, Message, Producer

log = logging.getLogger("producer")

# ----------------------------------------------------------------------
# 통계 (콜백에서 갱신)
# ----------------------------------------------------------------------
stats = {"sent": 0, "ok": 0, "fail": 0}
failed_log = None  # 실패 메시지 기록 파일 핸들
_stop = False


def delivery_report(err: KafkaError | None, msg: Message) -> None:
    """produce() 결과 콜백. 브로커가 ack 했을 때(또는 재시도 끝에 실패했을 때) 호출된다."""
    if err is not None:
        stats["fail"] += 1
        log.error("delivery failed key=%s err=%s", msg.key(), err)
        if failed_log:
            failed_log.write(json.dumps({"key": msg.key().decode(), "value": msg.value().decode(), "error": str(err)}) + "\n")
    else:
        stats["ok"] += 1
        if stats["ok"] <= 3:  # 처음 몇 건은 어디에 들어갔는지 보여준다
            log.info("delivered  topic=%s partition=%d offset=%d key=%s",
                     msg.topic(), msg.partition(), msg.offset(), msg.key().decode())


# ----------------------------------------------------------------------
# 메시지 빌드
# ----------------------------------------------------------------------
def ms_to_iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def build_message(row) -> tuple[str, dict]:
    """CSV 한 행 → (key, value dict).  key = visitorid (같은 유저 → 같은 파티션 → 순서 보장)."""
    tx = row.transactionid
    key = str(int(row.visitorid))
    value = {
        # 중복 제거 · 멱등 저장 기준. 원본엔 없어서 생성기가 부여하는데, 랜덤(uuid4)이면 리플레이를 다시 돌릴 때
        # 같은 행이 다른 ID를 받아 중복 제거가 안 된다. 행 내용(timestamp·visitorid·event·itemid)을 해시해서
        # 같은 행 → 항상 같은 ID 가 나오게 한다. (같은 ms 에 한 유저가 여러 상품을 본 행이 있어 itemid 까지 포함)
        "event_id": str(uuid.uuid5(uuid.NAMESPACE_OID, f"{int(row.timestamp)}|{int(row.visitorid)}|{row.event}|{int(row.itemid)}")),
        "event_type": row.event,                   # view | addtocart | transaction
        "event_time": ms_to_iso(int(row.timestamp)),  # 원본 발생 시각 → 윈도우 집계 기준
        "produced_at": now_iso(),                  # 생성기가 보낸 시각 → end-to-end 지연 측정 기준
        "schema_version": 1,
        "source": "replay-generator",
        "payload": {
            "visitor_id": int(row.visitorid),
            "item_id": int(row.itemid),
            "transaction_id": None if (isinstance(tx, float) and math.isnan(tx)) else int(tx),
        },
    }
    return key, value


# ----------------------------------------------------------------------
# 메인
# ----------------------------------------------------------------------
def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--file", default="data/events.csv")
    ap.add_argument("--topic", default="user-events")
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--speed", type=float, default=1.0, help="원본 간격 배속 (0 = 최대 속도)")
    ap.add_argument("--rate", type=float, default=None, help="초당 전송 건수 고정 (지정 시 --speed 무시)")
    ap.add_argument("--limit", type=int, default=None, help="보낼 최대 건수")
    ap.add_argument("--offset", type=int, default=0, help="CSV 앞에서 건너뛸 행 수")
    ap.add_argument("--failed-log", default="failed_messages.jsonl")
    ap.add_argument("--report-every", type=int, default=10000)
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stdout)

    global failed_log
    failed_log = open(args.failed_log, "a", encoding="utf-8")

    def _sig(*_):
        global _stop
        _stop = True
        log.warning("stop requested, flushing...")
    signal.signal(signal.SIGINT, _sig)

    # 1) 데이터 읽기 — 시간순 정렬 (원본 CSV는 시간순이 아님)
    log.info("loading %s", args.file)
    df = pd.read_csv(args.file)
    df = df.sort_values("timestamp", kind="stable").reset_index(drop=True)
    if args.offset:
        df = df.iloc[args.offset:]
    if args.limit:
        df = df.iloc[: args.limit]
    log.info("rows to send: %d", len(df))

    # 2) Producer 설정 — 안정성 우선
    producer = Producer({
        "bootstrap.servers": args.bootstrap,
        "client.id": "replay-generator",
        "enable.idempotence": True,   # 재시도해도 브로커에 중복 저장되지 않음 (acks=all, retries 자동 설정)
        "acks": "all",
        "compression.type": "lz4",
        "linger.ms": 5,               # 5ms 모아서 배치 전송 (6회차에 튜닝)
    })

    # 3) 전송 루프
    t0 = time.monotonic()
    prev_event_ms = None
    interval = (1.0 / args.rate) if args.rate else None
    next_due = time.monotonic()

    for row in df.itertuples(index=False):
        if _stop:
            break

        # --- 속도 제어 ---
        if interval is not None:                      # 고정 rate
            next_due += interval
            delay = next_due - time.monotonic()
            if delay > 0:
                time.sleep(delay)
        elif args.speed > 0 and prev_event_ms is not None:   # 원본 간격 리플레이
            gap = (int(row.timestamp) - prev_event_ms) / 1000.0 / args.speed
            if gap > 0:
                time.sleep(gap)
        prev_event_ms = int(row.timestamp)

        # --- 직렬화 ---
        key, value = build_message(row)
        payload = json.dumps(value, ensure_ascii=False).encode("utf-8")

        # --- 전송 (로컬 큐가 꽉 차면 콜백 처리하며 대기) ---
        while True:
            try:
                producer.produce(args.topic, key=key.encode("utf-8"), value=payload, callback=delivery_report)
                stats["sent"] += 1
                break
            except BufferError:
                producer.poll(0.1)
            except KafkaException as e:
                stats["fail"] += 1
                log.error("produce error key=%s err=%s", key, e)
                break

        producer.poll(0)  # 콜백 처리

        if stats["sent"] % args.report_every == 0:
            elapsed = time.monotonic() - t0
            log.info("sent=%d ok=%d fail=%d  %.0f msg/s", stats["sent"], stats["ok"], stats["fail"], stats["sent"] / elapsed)

    # 4) 남은 메시지 모두 전송 완료 대기
    remaining = producer.flush(30)
    elapsed = time.monotonic() - t0
    failed_log.close()
    log.info("DONE sent=%d ok=%d fail=%d unflushed=%d elapsed=%.1fs avg=%.0f msg/s",
             stats["sent"], stats["ok"], stats["fail"], remaining, elapsed, stats["sent"] / elapsed if elapsed else 0)


if __name__ == "__main__":
    main()
