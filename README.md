메타코드 데이터 엔지니어 7기 · 이유석

```
repo/
├─ docker-compose.yml   # Kafka (KRaft 단일 브로커)
├─ create_topics.py     # 토픽 생성 (user-events, user-events-dlq)
├─ producer.py          # events.csv 리플레이 -> Kafka  
├─ consumer.py          # 콘솔 consumer, 확인용      
├─ preprocess.py        # Spark 전처리 · 집계           
├─ dag.py               # Airflow DAG                  
├─ schema.sql           # PostgreSQL 스키마            
├─ data/events.csv      # Kaggle 에서 다운로드 (git 제외)
└─ docs/pipeline_architecture.png
```

---

## 1. 사용 데이터

**Retailrocket recommender system dataset** (Kaggle 공개, CC BY-NC-SA 4.0)
https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset

실제 이커머스 사이트에서 2015-05-03부터 4.5개월 동안 수집한 유저 행동 로그. 이 중 `events.csv` 사용

### 스키마 (`events.csv`)

| 필드 | 타입 | 설명 | 예시 |
|---|---|---|---|
| `timestamp` | int64 | 이벤트 발생 시각 (Unix epoch, 밀리초) | `1433221332117` |
| `visitorid` | int64 | 유저 ID (해시) | `257597` |
| `event` | string | `view` / `addtocart` / `transaction` | `view` |
| `itemid` | int64 | 상품 ID (해시) | `355908` |
| `transactionid` | int64 (nullable) | 구매 이벤트일 때만 채워지는 주문 ID | `4000` |

```csv
timestamp,visitorid,event,itemid,transactionid
1433221332117,257597,view,355908,
1433224214164,992329,view,248676,
1433222276276,599528,transaction,356475,4000
```
## 2. 전체 파이프라인 구조도

![pipeline architecture](docs/pipeline_architecture.png)

| 단계                      | 흐르는 것                             | 주기 |
|-------------------------|-----------------------------------|---|
| 생성기 -> Kafka            | JSON 이벤트 1건씩, key = visitor_id    | 리플레이 배속에 따라 (최대 수천 건/초) |
| Kafka -> Spark          | `user-events` 토픽 micro-batch      | 1분 트리거 |
| Spark -> PostgreSQL     | 정제 이벤트 upsert(event_id), 1분 집계    | 1분 |
| Spark -> Parquet        | 원본 append                         | 1분 |
| Airflow -> Spark/PostgreSQL | Parquet -> 일 집계, 추천 테이블 갱신, 품질 체크 | 매일 03:00 |
| PostgreSQL -> API       | 유저별 추천 Top-N, 트렌딩 아이템 SQL 조회      | 요청 시 |

---
### 3. Topic 구성

| 항목 | 값 | 이유                                                                         |
|---|---|----------------------------------------------------------------------------|
| 토픽명 | `user-events` | view · addtocart · transaction 와 같이 이벤트 종류가 달라도 같은 유저 안에서 발생 순서대로 읽어야 하기떄문 |
| 파티션 수 | 3 | 유저가 30명이면 각 파티션에 10명씩 들어가서 병렬처리로 성능을 올린다            파티션은 0,1,2 가존재         |
| key | `visitor_id` | visitor id %3 으로 파티션에 넣는다                                                  |
| 리텐션 | 7일  | 집계를 실수해서 데이터를 다시 봐야하는 상황이 그렇게 많지는 않을거같아서 7일로 했다.                           |
| DLQ | `user-events-dlq` (파티션 1, 리텐션 30일) | 두번째 토픽으로 메세지가 꺠져서 읽기가 실패한 데이터들을 모아둔 토픽                                     |

```
$ kafka-topics.sh --describe --topic user-events
Topic: user-events  PartitionCount: 3  ReplicationFactor: 1  Configs: retention.ms=604800000
    Partition: 0  Leader: 1  Replicas: 1  Isr: 1
    Partition: 1  Leader: 1  Replicas: 1  Isr: 1
    Partition: 2  Leader: 1  Replicas: 1  Isr: 1
```

**리스너 구성** (`docker-compose.yml`): 내 PC의 Python은 `localhost:9092`, 같은 compose 네트워크의 Spark · Airflow 컨테이너는 `kafka:29092`로 접속하도록 리스너를 두 개 열어 뒀다.
   

## 4. Kafka 수집 

### 4-1. Producer (`producer.py`)

```
CSV 읽기 -> 시간순 정렬 ->  (속도 제어: sleep) ->  JSON 직렬화 ->  produce(key, value) ->  콜백
                                                                                           ├─ 성공: ok 카운트, 처음 3건은 partition/offset 로그
                                                                                           └─ 실패: fail 카운트, failed_messages.jsonl 에 기록
```
![producer_flow.png](docs/producer_flow.png)

**Producer 설정**

| 설정 | 값 |
|---|---|---|
| `enable.idempotence` | `true` 
| `acks` | `all` 
| `compression.type` | `lz4` 
| `linger.ms` | `5`
   

### 4-3. message examples

key = `"1149227"` (visitorid)

```json
{
  "event_id": "bf7a5319-89f5-45d6-b21a-758d81e99189",
  "event_type": "view",  
  "event_time": "2015-05-03T03:00:27.031Z",
  "produced_at": "2026-10-26T02:33:54.794Z",
  "schema_version": 1,
  "source": "replay-generator",
  "payload": {
    "visitor_id": 1149227,
    "item_id": 29757,
    "transaction_id": null
  }
}

```

**producer 실행 로그** 

```
INFO loading data/events.csv
INFO rows to send: 20000
INFO sent=5000 ok=0 fail=0  63280 msg/s
INFO sent=10000 ok=0 fail=0  62865 msg/s
INFO sent=20000 ok=0 fail=0  64246 msg/s
INFO delivered  topic=user-events partition=1 offset=0 key=693516
INFO delivered  topic=user-events partition=1 offset=1 key=652699
INFO delivered  topic=user-events partition=1 offset=2 key=693516
INFO DONE sent=20000 ok=20000 fail=0 unflushed=0 elapsed=1.1s avg=17899 msg/s
```

**`--rate 200` 실행** 

```
INFO rows to send: 1000
INFO sent=500 ok=498 fail=0  200 msg/s
INFO sent=1000 ok=999 fail=0  200 msg/s
INFO DONE sent=1000 ok=1000 fail=0 unflushed=0 elapsed=5.0s avg=200 msg/s
```

**consumer 로 읽은 결과** 

```
[p2 off=0] key=1149227  view        item=29757   event_time=2015-05-03T03:00:27.031Z
[p2 off=1] key=1150418  view        item=374675  event_time=2015-05-03T03:00:38.570Z
[p2 off=2] key=1143053  view        item=463330  event_time=2015-05-03T03:00:48.174Z
[p2 off=3] key=1222029  view        item=75831   event_time=2015-05-03T03:00:51.805Z
[p2 off=4] key=990356   view        item=161722  event_time=2015-05-03T03:00:54.461Z
```

---
## 5. 전처리 · 저장

Kafka `user-events` → Spark Structured Streaming(`preprocess.py`) → Parquet(전체 이력 보관) + PostgreSQL(최근 30일, 조회용).
처리 흐름 · 입출력 예시 · 최종 스키마 · 저장 포맷 선택 이유는 `docs/전처리_저장.pdf` 참고.

**두 저장소의 역할**

| 저장소 | 보관 범위 | 역할 |
|---|---|---|
| Parquet `data/parquet/events/dt=…` | 전체. 지우지 않음 | 원본 이력. 집계 로직을 고쳐 재처리하거나 과거 구간을 분석할 때 여기서 읽는다 |
| PostgreSQL `events`, `item_stats_1m` | 가장 최근 이벤트 기준 30일 (`--pg-retention-days`) | 조회용. 유저별 · 아이템별 인덱스, API 가 읽는 곳. 배치마다 기간 밖 행을 삭제해 가볍게 유지 |

보관 기준이 `now()` 가 아니라 "저장된 가장 최근 이벤트 시각" 인 이유: 2015년 로그를 리플레이하는 중이라 `now()` 기준이면 전부 지워진다. 실시간 수집이면 둘이 같다.

### 5-1. 실행 방법

```bash
# 0) 준비 — Python 3.10 + Java 17/21 (pyspark 3.5.3 은 Python 3.13 에서 워커가 죽는다)
pip install -r requirements.txt

# 1) Kafka + PostgreSQL 기동 (postgres 는 첫 기동 때 schema.sql 을 자동 실행해서 테이블을 만든다)
docker compose up -d
python create_topics.py                       # user-events, user-events-dlq

# 2) 수집
python producer.py --limit 919497 --speed 0   # CSV 앞 45일치 리플레이 → Kafka (24초)
python consumer.py --max 5                    # 들어갔는지 눈으로 확인

# 3) 전처리 · 저장
python preprocess.py                          # 쌓인 메시지를 전부 처리하고 종료 (trigger availableNow)
python preprocess.py --continuous             # 1분마다 micro-batch 를 계속 돌림 (Ctrl+C 종료)
python preprocess.py --reset                  # 체크포인트 삭제 후 토픽 처음부터 재처리 (재실행 테스트용)
python preprocess.py --pg-retention-days 7    # PostgreSQL 보관 기간 변경 (기본 30)

# 4) 결과 조회
docker exec postgres psql -U clickstream -d clickstream -c "select min(event_time)::date, max(event_time)::date, count(*) from events;"
docker exec postgres psql -U clickstream -d clickstream -c "select * from item_stats_1m order by view_cnt desc limit 5;"
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic user-events-dlq --from-beginning --timeout-ms 5000
```

최초 실행 때 Spark 가 Kafka 커넥터와 PostgreSQL JDBC 드라이버를 Maven 에서 내려받는다 (인터넷 필요, 1회).

### 5-2. 입력 · 출력 위치

| 구분 | 위치 | 형식 | 비고 |
|---|---|---|---|
| 입력 | Kafka `user-events` (`localhost:9092`) | JSON 메시지, key = visitor_id | producer.py 가 쌓은 것 |
| 진행 위치 | `checkpoint/preprocess/` | Spark 체크포인트 (Kafka offset) | 지우면 처음부터 다시 읽음 |
| 출력 ① 전체 이력 | `data/parquet/events/dt=YYYY-MM-DD/*.parquet` | Parquet, 날짜 파티션, append | 지우지 않음. 재처리 입력 |
| 출력 ② 조회용 | PostgreSQL `localhost:5432` db `clickstream` (user/pw `clickstream`) | `events`, `item_stats_1m` 테이블 | 최근 30일만. `events_staging` 은 배치 적재용 임시 테이블 |
| 출력 ③ 실패 격리 | Kafka `user-events-dlq` | 원문 + `dlq_reason` + partition/offset | 파싱 · 검증 실패 메시지 |

**실제 실행 결과** (CSV 앞 45일치 919,497건을 Kafka 에 넣고 한 번에 처리)

```
INFO batch=0 kafka=919497 parsed_ok=919497 dlq=0 dedup=919381(-116) | pg inserted=919381 skipped_existing=0 stats_upsert=887413 | retention(30d, latest=2015-06-17 03:00:01.600000+00:00) deleted events=301050 stats=290728 | parquet=data/parquet/events | 62.5s
INFO DONE 쌓인 메시지 처리 완료
```

| 확인 | 결과 |
|---|---|
| 중복 제거 | 원본 CSV 에 완전히 같은 행이 116건 있어 `event_id` 가 같음 → 하나만 남김 (919,497 → 919,381) |
| Parquet (전체) | 2015-05-03 ~ 06-17, 46일, 919,381행, 738파일, 54MB |
| PostgreSQL `events` (최근 30일) | 2015-05-18 ~ 06-17, 618,331행 (view 598,704 · addtocart 14,807 · transaction 4,820). 기간 밖 301,050행 삭제됨 |
| PostgreSQL `item_stats_1m` | 596,685행, 같은 기간 |
| 깨진 메시지 주입 테스트 | JSON 아님 · `event_type=purchase` 2건 → `dlq=2`, `user-events-dlq` 에 `invalid_json`, `bad_event_type` 이유와 함께 격리 |

**같은 입력을 다시 처리하면** (23,000건으로 검증)

| 상황 | 결과 |
|---|---|
| 그냥 다시 실행 (체크포인트 유지) | 새 offset 이 없어서 아무것도 처리 안 함. 저장 결과 변화 없음 |
| `--reset` 으로 처음부터 재처리 | `pg inserted=0 skipped_existing=23000`. `events` 는 PK 로 그대로, `item_stats_1m` 은 재계산이라 내용 md5 가 전후 동일 |
| 단, Parquet | append 라 행이 두 배 (distinct event_id 는 그대로). 일 배치 `make_parquet` 가 날짜별로 지운다 (6장) |

---

## 6. 일 배치 (Airflow)

스트리밍(`preprocess.py`)이 쌓아 둔 하루치 데이터를 매일 한 번 정리 → 집계 → 검사 → 공개하는 DAG (`dags/dag.py`, dag_id `daily_events`).
스트리밍은 "빨리, 조금씩", 일 배치는 "하루가 끝난 뒤 정확하게" 를 맡는다.

### 6-1. DAG 구조

```
check_partition ──▶ make_parquet ──▶ daily_agg ──▶ data_check ──▶ publish
   (날짜 확인)      (파일 합치기·중복 제거)  (하루 상품 집계)  (건수·null·중복)   (대시보드 갱신)
                          └──── 어느 Task 든 최종 실패하면 → 알림 (on_failure_callback) ────┘
```

| Task | 하는 일 (한 줄) | 입력 → 출력 | 재시도 |
|---|---|---|---|
| `check_partition` | 처리할 날짜를 정하고 그 날짜 Parquet 폴더가 있는지 확인. 없으면 이후 Task 를 건너뜀(skip) | `dt=YYYY-MM-DD/` 존재 여부 | 0 |
| `make_parquet` | 그 날짜의 작은 파일들을 하나로 합치고 `event_id` 중복을 지운 뒤 폴더를 통째로 교체 | Parquet 여러 개 → `combined.parquet` 1개 | 3 |
| `daily_agg` | 하루 × 상품별 view · addtocart · transaction · 고유 방문자 수를 다시 계산 | Parquet → PostgreSQL `item_stats_daily` | 3 |
| `data_check` | 건수 > 0, 필수 컬럼 null 0, `event_id` 중복 0, 이벤트 종류 · 날짜 범위, 집계 합계 = 원본 건수 | Parquet + `item_stats_daily` → 통과 / 실패 | 0 |
| `publish` | 검사를 통과한 날짜만 하루 요약 1행을 업서트 | Parquet → PostgreSQL `daily_summary` | 3 |

`daily_agg` 를 PostgreSQL `events` 가 아니라 Parquet 에서 계산하는 이유: `events` 는 최근 30일만 남기 때문. Parquet 은 전체 이력이라 오래된 날짜도 같은 코드로 다시 계산할 수 있다.

**같은 날짜를 두 번 돌려도 결과가 같게 (멱등)**

| 대상 | 방법 |
|---|---|
| Parquet `dt=…/` | 합친 파일을 `_temp/dt=…/` 에 다 쓴 뒤, 원본 폴더를 `_old/` 로 옮기고 `_temp` 를 그 자리로 옮긴 다음 `_old` 를 지운다 (append 아님). 중간에 죽으면 다음 실행의 `check_partition` 이 `_old` 를 제자리로 되돌린다. 행을 정렬해서 쓰므로 파일 내용도 매번 같다 |
| `item_stats_daily` | 한 트랜잭션 안에서 `DELETE WHERE dt = …` + `INSERT` |
| `daily_summary` | PK(`dt`) 로 `INSERT … ON CONFLICT DO UPDATE` |

`_temp` 처럼 `_` 로 시작하는 폴더는 Spark · pyarrow 가 읽지 않으므로 작업 중인 파일이 데이터에 섞이지 않는다.
`make_parquet` 가 5-3 의 "Parquet 재처리 중복" 을 날짜 단위로 해결한다. 스트리밍을 처음부터 다시 돌려 같은 이벤트가 또 쌓여도, 그 날짜 DAG 가 돌면 중복이 사라진다.

### 6-2. retry · backoff 전략

기본값: `retries=3`, `retry_delay=1분`, `retry_exponential_backoff=True` → 재시도 전 대기 시간이 1~2분 → 2~4분 → 4~8분으로 늘어난다 (Airflow 가 범위 안에서 랜덤으로 고른다).

| 구분 | Task | 이유 |
|---|---|---|
| 재시도한다 | `make_parquet`, `daily_agg`, `publish` | 파일 잠금, DB 연결 끊김, 잠깐의 부하처럼 기다리면 풀리는 실패가 대부분. 간격을 늘려 복구 중인 DB 를 계속 두드리지 않는다 |
| 재시도 없이 바로 알림 | `data_check` | 데이터가 틀린 것은 다시 돌려도 똑같이 틀린다. `AirflowFailException` 으로 즉시 실패 |
| 재시도 없음 | `check_partition` | 폴더가 없으면 실패가 아니라 "처리할 것 없음" → skip |

알림: 재시도를 다 쓰고 최종 실패하면 `on_failure_callback`(`notify_fail`) 이 실행된다. `DISCORD_WEBHOOK_URL` 환경 변수가 있으면 Discord 로 보내고, 없으면 `ALERT` 로그만 남긴다.

### 6-3. 스케줄 · 의존성

| 항목 | 값 |
|---|---|
| 실행 시각 | 매일 02:00 UTC (`schedule="0 2 * * *"`) |
| 처리 날짜 | 실행 전날 하루치 (UTC 00:00 ~ 24:00) = `data_interval_start` 의 날짜. 예: 10/31 02:00 실행 → 10/30 처리 |
| 시간대 | UTC 로 통일. Spark 가 `dt` 파티션을 UTC 날짜로 만들기 때문 |
| 왜 02:00 | 자정 직후엔 늦게 도착한 전날 이벤트가 아직 들어올 수 있어서 2시간 여유를 둔다 |
| 동시 실행 | `max_active_runs=1`. 같은 폴더를 두 실행이 동시에 교체하지 않도록 |
| catchup | `False`. 켜는 순간 2015년부터 수천 번 실행되는 것을 막는다. 과거 날짜는 backfill 로 |

**리플레이 데이터의 날짜 연결 규칙**: 처리 날짜는 실행 날짜가 아니라 `target_date` 파라미터(없으면 `data_interval_start` 의 날짜)로 정한다. 2015년 데이터는 `target_date` 를 직접 주거나, backfill 의 logical date 를 2015년 날짜로 줘서 처리한다.

**순서의 이유**

1. `check_partition` 이 맨 앞 — 데이터가 없는 날에 뒤 Task 들이 빈 결과를 쓰지 않도록
2. `make_parquet` → `daily_agg` — 중복을 지운 파일로 집계해야 숫자가 맞다
3. `daily_agg` → `data_check` — 원본과 집계 결과를 둘 다 만든 뒤에 "합계가 같은가" 를 볼 수 있다
4. `data_check` → `publish` — 검사에 실패한 날짜는 대시보드에 올라가지 않는다

### 6-4. 실행 방법

```bash
# 1) Airflow 기동 (공식 docker compose 를 LocalExecutor 로 줄인 것). 기존 compose 와 같이 띄운다
docker compose -f docker-compose.yml -f docker-compose.airflow.yml up -d
#    UI: http://localhost:8080  (airflow / airflow)

# 2) 테이블 추가 (postgres 볼륨이 이미 있으면 schema.sql 이 자동 실행되지 않으므로 한 번 직접)
docker exec -i postgres psql -U clickstream -d clickstream < schema.sql

# 3) DAG 켜기 + 실행
docker exec airflow-scheduler airflow dags unpause daily_events
docker exec airflow-scheduler airflow dags trigger daily_events -c '{"target_date": "2015-06-10"}'   # 하루
docker exec airflow-scheduler airflow dags backfill daily_events -s 2015-05-03 -e 2015-06-18       # 여러 날 (05-03 ~ 06-17, -e 날짜 자체는 안 돈다)

# 4) 결과
docker exec postgres psql -U clickstream -d clickstream -c "select * from daily_summary order by dt;"
```

| 구분 | 위치 |
|---|---|
| DAG | `dags/dag.py` |
| Airflow 설정 | `docker-compose.airflow.yml` (메타 DB 는 별도 `airflow-db` 컨테이너) |
| 입력 | `data/parquet/events/dt=YYYY-MM-DD/` (컨테이너 안 `/opt/airflow/data/parquet/events`) |
| 출력 | Parquet 같은 폴더 (`combined.parquet` 1개로 교체), PostgreSQL `item_stats_daily`, `daily_summary` |

### 6-5. 실행 결과

**같은 날짜(2015-06-10)를 두 번 실행 → 결과 동일**

```
              Parquet 파일                 item_stats_daily                          daily_summary
1차 실행      combined.parquet md5 63ea045f8285   13,985행 합계 24,020 md5 851b57b02c1c     24020 view 23213 cart 597 tx 210 방문자 13995
2차 실행      combined.parquet md5 63ea045f8285   13,985행 합계 24,020 md5 851b57b02c1c     (동일)
```

로그: `daily_agg items=13985 events=24020` · `quality 전부 0, agg_total=24020`.

**스트리밍 재처리로 중복이 쌓인 상황을 만들고 다시 실행 → 결과 동일**

Spark 가 쓴 원본 파일 하나를 같은 폴더에 다시 넣어 같은 이벤트 2,984건을 중복시킨 뒤 실행:
`compact {'parquet_cnt': 2, 'files_after': 1, 'rows_cnt': 27004, 'rows_after': 24020, 'duplicates_removed': 2984}`. 파일 md5 · 집계 md5 · 요약 모두 1차와 같다.

**품질 검사 실패 → 재시도 없이 바로 알림, 대시보드에는 안 올라감**

2015-06-11 폴더에 `event_type="purchase"` 인 행 1개를 넣고 실행:

```
data_check  FAILED  try=1  (Immediate failure requested)
ALERT `daily_events.data_check` 실패 (target_date=2015-06-11, try=1)
      — 품질 검사 실패: ['bad_event_type', 'agg_total != row_count'] {'row_count': 20908, 'bad_event_type': 1, 'agg_total': 20907, ...}
      (이상한 종류 1건은 집계에서 빠지므로 합계가 원본보다 1 적다 → 두 검사 모두에 걸림)
publish     upstream_failed
```

테스트 후 원본 폴더로 되돌리고 다시 실행 → success, `daily_summary` 2015-06-11 = 20,907건.

**재시도 간격**: 파일 합치기 Task 가 읽기 오류(테스트 파일을 다른 pyarrow 버전으로 만듦)로 두 번 실패했을 때 1분 19초 뒤, 약 4분 뒤 재시도 → 세 번째에 성공. 대기 시간이 늘어나는 것을 확인했다.

**스케줄 실행 (데이터 없는 날)**: `scheduled__2026-10-31` 실행은 `dt=2026-10-31` 폴더가 없어서 `check_partition` 이 skip → 나머지도 skip, 실행 상태는 success.

**전체 기간 backfill (2015-05-03 ~ 06-17, 46일)**: 46회 × 5 Task = 230개 전부 success, 총 약 2분 30초.

| 확인 | 결과 |
|---|---|
| Parquet | 46개 폴더에 `combined.parquet` 하나씩 (처음 합칠 때 Spark 파일 738개 → 46개), 54MB → 50MB, 919,381행 = distinct `event_id` 919,381 |
| Spark 로 다시 읽기 | `spark.read.parquet` 919,381행, `dt` 와 `event_time` 날짜가 다른 행 0. 합친 파일과 Spark 파일이 섞여도 읽힌다 |
| `item_stats_daily` | 553,646행, 46일, 합계 919,381 (= Parquet 전체 건수) |
| `daily_summary` | 46행. 합계 view 889,821 · addtocart 22,307 · transaction 7,253 |

`daily_summary` 는 PostgreSQL `events`(최근 30일) 보다 긴 46일을 다 갖고 있다. Parquet 에서 계산했기 때문.

---

## 7. 로드 테스트 · 장애 대응

### 7-1. 측정

| 지표 | 계산 | 코드 |
|---|---|---|
| 입력 · 처리 속도, 배치 시간 | Spark `query.lastProgress` | `preprocess.py` `progress_logger` |
| lag | 보낸 건수 − 처리한 건수 | `loadtest/report.py` |
| 지연 p95 | DB 저장 시각 − 보낸 시각 | `preprocess.py` `LATENCY_SQL` |
| 구간별 시간 | Spark / Parquet / PostgreSQL 각각 | `preprocess.py` 배치 핸들러 |

```bash
python preprocess.py --continuous --trigger-seconds 10 --checkpoint checkpoint/loadtest --starting-offsets latest --parquet-dir data/parquet/loadtest
bash loadtest/run_steps.sh 919497 100 300 1000 3000
bash loadtest/run_failure_db.sh 2400000
bash loadtest/run_failure_spark.sh 2550000
python loadtest/report.py --drain-seconds 60
```

### 7-2. 부하 (단계 증가, 각 2분, 트리거 10초)

| 입력 (건/s) | 처리 (건/s) | 배치 시간 평균 / 최대 | 지연 p95 | lag 최대 | 판단 |
|---|---|---|---|---|---|
| 100 | 608 | 1.9s / 5.5s | 10.7s | 734 | 여유 |
| 300 | 1,470 | 2.0s / 2.2s | 10.5s | 1,330 | 여유 |
| 1,000 | 1,508 | 6.4s / 7.0s | 10.8s | 7,879 | 여유 |
| 3,000 | 7,377 | 4.1s / 9.0s | 11.6s | 16,804 | 여유 |
| 6,000 | 10,033 | 6.0s / 11.1s | 12.2s | 76,416 | 여유 |
| 10,000 | 12,463 | 7.2s / 7.9s | 14.3s | 64,966 | 여유 |
| 20,000 | 9,614 | 40.5s / 63.0s | 44.3s | 1,048,636 | **한계** |

- 지연 p95 바닥 10초 = 트리거 간격
- 처리 상한 약 13,000건/s. 20,000/s 에서 배치가 10초를 넘겨 lag 가 계속 쌓임
- **병목**: PostgreSQL 저장 47%, Spark 파싱 46%, Parquet 6%

### 7-3. 장애

| 장애 | 대응 |
|---|---|
| DB 정지 | 5s → 10s → 20s → 30s 간격으로 최대 6번 재시도. `ON CONFLICT` 라 중복 없음 |
| Spark 죽음 | 체크포인트에서 이어서 처리 |
| 깨진 메시지 | DLQ 토픽으로 격리 |
| 입력 폭증 | `--max-offsets-per-trigger` 로 배치 크기 제한 |
| 알림 | 재시도 WARNING, 최종 실패 ERROR 로그. 일 배치는 Discord 웹훅 |

**DB 60초 정지** (1,000/s, 150,000건)

```
11:01:16  stop postgres → 재시도 4번 (5s, 10s, 20s, 30s)
11:02:18  start postgres
11:02:26  저장 성공, 밀린 77,061건 8.7s 에 처리
11:02:42  지연 p95 10.5s 복귀
```

결과: 149,970건 저장 (CSV 중복 30건 제외). 유실 0, 중복 0.

**Spark 강제 종료 → 15초 뒤 재시작** (1,000/s, 120,000건)

```
11:04:43  kill
11:05:07  재시작, 밀린 39,501건 4.5s 에 처리
11:05:27  지연 p95 6.0s 복귀
```

결과: 119,987건 저장 (CSV 중복 13건 제외). 유실 0, 중복 0.
