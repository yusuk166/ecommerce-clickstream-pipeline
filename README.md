메타코드 데이터 엔지니어 7기 · 이유석

```
repo/
├─ docker-compose.yml           # Kafka · PostgreSQL
├─ docker-compose.airflow.yml   # Airflow
├─ create_topics.py             # 토픽 생성
├─ producer.py                  # events.csv → Kafka
├─ consumer.py                  # 확인용 consumer
├─ preprocess.py                # Spark 전처리 · 저장
├─ dags/dag.py                  # Airflow 일 배치
├─ loadtest/                    # 로드 테스트 스크립트
├─ schema.sql                   # PostgreSQL 스키마
└─ data/events.csv              # Kaggle (git 제외)
```

## 1. 데이터

[Retailrocket recommender system dataset](https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset) 의 `events.csv`. 2015-05-03 부터 4.5개월, 2,756,101건.

| 필드 | 타입 | 뜻 |
|---|---|---|
| `timestamp` | int64 | 발생 시각 (ms) |
| `visitorid` | int64 | 유저 |
| `event` | string | `view` / `addtocart` / `transaction` |
| `itemid` | int64 | 상품 |
| `transactionid` | int64, null | 구매일 때만 |

```csv
timestamp,visitorid,event,itemid,transactionid
1433221332117,257597,view,355908,
1433222276276,599528,transaction,356475,4000
```

## 2. 구조

![pipeline architecture](docs/pipeline_architecture.png)

| 단계 | 흐르는 것 | 주기 |
|---|---|---|
| producer → Kafka | JSON 1건씩, key = visitor_id | 리플레이 속도 |
| Kafka → Spark | `user-events` micro-batch | 1분 |
| Spark → PostgreSQL | `events` upsert, 1분 집계 | 1분 |
| Spark → Parquet | 원본 append | 1분 |
| Airflow | Parquet → 일 집계 · 품질 체크 · 대시보드 | 매일 02:00 |

## 3. Topic

| 항목 | 값 | 이유 |
|---|---|---|
| 토픽 | `user-events` | 이벤트 종류가 달라도 같은 유저 안에서 발생 순서대로 읽어야 해서 하나로 |
| 파티션 | 3 | 병렬 처리 |
| key | `visitor_id` | 같은 유저 → 같은 파티션 → 순서 보장 |
| 리텐션 | 7일 | 재처리는 Parquet 에서 |
| DLQ | `user-events-dlq` (파티션 1, 30일) | 깨진 메시지 격리 |

리스너: 내 PC 는 `localhost:9092`, 컨테이너는 `kafka:29092`.

## 4. 수집 (`producer.py`)

```
CSV 읽기 → 시간순 정렬 → 속도 제어 (--speed, --rate) → JSON → produce(key, value) → 콜백
```

![producer_flow.png](docs/producer_flow.png)

| 설정 | 값 |
|---|---|
| `enable.idempotence` | `true` |
| `acks` | `all` |
| `compression.type` | `lz4` |
| `linger.ms` | `5` |

`event_id` = uuid5(timestamp, visitorid, event, itemid). 같은 행은 항상 같은 ID → 재전송 · 원본 중복 제거.

```json
{
  "event_id": "bf7a5319-89f5-45d6-b21a-758d81e99189",
  "event_type": "view",
  "event_time": "2015-05-03T03:00:27.031Z",
  "produced_at": "2026-10-26T02:33:54.794Z",
  "schema_version": 1,
  "source": "replay-generator",
  "payload": {"visitor_id": 1149227, "item_id": 29757, "transaction_id": null}
}
```

```
INFO DONE sent=20000 ok=20000 fail=0 unflushed=0 elapsed=1.1s avg=17899 msg/s
```

## 5. 전처리 · 저장 (`preprocess.py`)

```
Kafka → 파싱 → 검증 (실패는 DLQ) → 중복 제거 → Parquet (전체) + PostgreSQL (최근 30일)
```

| 저장소 | 범위 | 역할 |
|---|---|---|
| Parquet `data/parquet/events/dt=…` | 전체 | 원본 이력, 재처리 입력 |
| PostgreSQL `events`, `item_stats_1m` | 최근 30일 (최신 이벤트 기준) | 조회용 |
| Kafka `user-events-dlq` | 30일 | 파싱 · 검증 실패 메시지 |

PostgreSQL 은 staging → `INSERT … ON CONFLICT (event_id) DO NOTHING` → 1분 집계 재계산 → 30일 밖 삭제. 몇 번 돌려도 같다.

```bash
pip install -r requirements.txt          # Python 3.10, Java 17
docker compose up -d
python create_topics.py
python producer.py --limit 919497 --speed 0
python preprocess.py                     # 쌓인 것 처리 후 종료
python preprocess.py --continuous        # 1분마다
python preprocess.py --reset             # 처음부터
```

**결과** (45일치 919,497건)

| 확인 | 결과 |
|---|---|
| 중복 제거 | 116건 (원본에 같은 행) → 919,381 |
| Parquet | 46일, 919,381행 |
| PostgreSQL `events` | 최근 30일 618,331행 |
| DLQ | 깨진 메시지 2건 격리 (`invalid_json`, `bad_event_type`) |
| `--reset` 재처리 | `inserted=0 skipped_existing=23000`, 집계 md5 동일 |

## 6. 일 배치 (Airflow, `dags/dag.py`)

```
check_partition → make_parquet → daily_agg → data_check → publish
```

| Task | 하는 일 | 재시도 |
|---|---|---|
| `check_partition` | 처리할 날짜 정하고 폴더 확인. 없으면 전부 skip | 0 |
| `make_parquet` | 파일 합치기 + `event_id` 중복 제거 → `combined.parquet` 1개로 교체 | 3 |
| `daily_agg` | 하루 × 상품 집계 → `item_stats_daily` (DELETE + INSERT) | 3 |
| `data_check` | 건수 · null · 중복 · 종류 · 날짜 · 집계 합계 검사. 실패 시 바로 알림 | 0 |
| `publish` | 하루 요약 → `daily_summary` (PK 업서트) | 3 |

| 항목 | 값 |
|---|---|
| 스케줄 | 매일 02:00 UTC, 전날 하루치 (`data_interval_start`) |
| 2015년 데이터 | `target_date` 파라미터 또는 backfill |
| retry | 3회, 1분 → 2분 → 4분 (`retry_exponential_backoff`) |
| 알림 | `on_failure_callback` → Discord 웹훅 (없으면 로그) |
| Parquet 교체 | `_temp` 에 쓰고 → 원본 `_old` 로 → `_temp` 를 제자리로 → `_old` 삭제. 중간에 죽으면 다음 실행이 `_old` 복구 |

```bash
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d     # UI localhost:8080 (airflow / airflow)
docker exec -i postgres psql -U clickstream -d clickstream < schema.sql
docker exec airflow-scheduler airflow dags unpause daily_events
docker exec airflow-scheduler airflow dags trigger daily_events -c '{"target_date": "2015-06-10"}'
docker exec airflow-scheduler airflow dags backfill daily_events -s 2015-05-03 -e 2015-06-18
```

**결과**

| 확인 | 결과 |
|---|---|
| 같은 날짜 2번 실행 | Parquet md5 · 집계 md5 · 요약 동일 |
| 중복 2,984건 넣고 실행 | 27,004 → 24,020행, 결과 동일 |
| 잘못된 행 1건 넣고 실행 | `data_check` 첫 시도에 실패, `publish` 안 돎, ALERT 로그 |
| 데이터 없는 날 | 전부 skip, success |
| 46일 backfill | 230 Task 전부 success, 2분 30초 |

## 7. 로드 테스트 · 장애 대응

| 지표 | 계산 | 코드 |
|---|---|---|
| 입력 · 처리 속도, 배치 시간 | Spark `query.lastProgress` | `preprocess.py` `progress_logger` |
| lag | 보낸 건수 − 처리한 건수 | `loadtest/report.py` |
| 지연 p95 | DB 저장 시각 − 보낸 시각 | `preprocess.py` `LATENCY_SQL` |
| 구간별 시간 | Spark / Parquet / PostgreSQL | `preprocess.py` 배치 핸들러 |

```bash
python preprocess.py --continuous --trigger-seconds 10 --checkpoint checkpoint/loadtest --starting-offsets latest --parquet-dir data/parquet/loadtest
bash loadtest/run_steps.sh 919497 100 300 1000 3000
bash loadtest/run_failure_db.sh 2400000
bash loadtest/run_failure_spark.sh 2550000
python loadtest/report.py --drain-seconds 60
```

**부하** (단계 증가, 각 2분, 트리거 10초)

| 입력 (건/s) | 처리 (건/s) | 배치 시간 평균 / 최대 | 지연 p95 | lag 최대 | 판단 |
|---|---|---|---|---|---|
| 100 | 608 | 1.9s / 5.5s | 10.7s | 734 | 여유 |
| 300 | 1,470 | 2.0s / 2.2s | 10.5s | 1,330 | 여유 |
| 1,000 | 1,508 | 6.4s / 7.0s | 10.8s | 7,879 | 여유 |
| 3,000 | 7,377 | 4.1s / 9.0s | 11.6s | 16,804 | 여유 |
| 6,000 | 10,033 | 6.0s / 11.1s | 12.2s | 76,416 | 여유 |
| 10,000 | 12,463 | 7.2s / 7.9s | 14.3s | 64,966 | 여유 |
| 20,000 | 13,639 ~ 16,651 | 40.5s / 63.0s | 44.3s | 1,048,636 | **한계** |

- 지연 p95 바닥 10초 = 트리거 간격
- 처리 상한 약 14,000~17,000건/s. 20,000/s 에서 입력보다 느려 배치가 10초를 넘기고 lag 가 계속 쌓임
- **병목**: PostgreSQL 저장 47%, Spark 파싱 46%, Parquet 6%

**장애**

| 장애 | 대응 |
|---|---|
| DB 정지 | 5s → 10s → 20s → 30s 간격으로 최대 6번 재시도. `ON CONFLICT` 라 중복 없음 |
| Spark 죽음 | 체크포인트에서 이어서 처리 |
| 깨진 메시지 | DLQ 토픽으로 격리 |
| 입력 폭증 | `--max-offsets-per-trigger` 로 배치 크기 제한 |
| 알림 | 재시도 WARNING, 최종 실패 ERROR 로그. 일 배치는 Discord 웹훅 |

DB 60초 정지 (1,000/s, 150,000건)

```
11:01:16  stop postgres → 재시도 4번 (5s, 10s, 20s, 30s)
11:02:18  start postgres
11:02:26  저장 성공, 밀린 77,061건 8.7s 에 처리
11:02:42  지연 p95 10.5s 복귀
```

149,970건 저장 (CSV 중복 30건 제외). 유실 0, 중복 0.

Spark 강제 종료 → 15초 뒤 재시작 (1,000/s, 120,000건)

```
11:04:43  kill
11:05:07  재시작, 밀린 39,501건 4.5s 에 처리
11:05:27  지연 p95 6.0s 복귀
```

119,987건 저장 (CSV 중복 13건 제외). 유실 0, 중복 0.
