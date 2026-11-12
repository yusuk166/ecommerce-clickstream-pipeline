
CREATE TABLE IF NOT EXISTS events (
    event_id        UUID         PRIMARY KEY,          -- 생성기가 행 내용(timestamp|visitorid|event|itemid)으로 만든 uuid5
    event_type      TEXT         NOT NULL CHECK (event_type IN ('view', 'addtocart', 'transaction')),
    event_time      TIMESTAMPTZ  NOT NULL,             -- 원본 발생 시각 (UTC). 집계 기준
    produced_at     TIMESTAMPTZ  NOT NULL,             -- 생성기가 보낸 시각. end-to-end 지연 측정용
    visitor_id      BIGINT       NOT NULL,
    item_id         BIGINT       NOT NULL,
    transaction_id  BIGINT,                            -- transaction 일 때만 값이 있음
    schema_version  INT          NOT NULL,
    source          TEXT         NOT NULL,
    ingested_at     TIMESTAMPTZ  NOT NULL DEFAULT now() 
);

CREATE INDEX IF NOT EXISTS idx_events_time ON events (event_time);
CREATE INDEX IF NOT EXISTS idx_events_visitor_time ON events (visitor_id, event_time);
CREATE INDEX IF NOT EXISTS idx_events_item_time    ON events (item_id, event_time);


-- Spark 가 micro-batch 를 JDBC 로 통째로 밀어 넣는 임시 테이블.
-- 배치마다 overwrite 되고, 여기서 events 로 ON CONFLICT 로 옮긴다.
CREATE TABLE IF NOT EXISTS events_staging (
    event_id        UUID,
    event_type      TEXT,
    event_time      TIMESTAMPTZ,
    produced_at     TIMESTAMPTZ,
    visitor_id      BIGINT,
    item_id         BIGINT,
    transaction_id  BIGINT,
    schema_version  INT,
    source          TEXT
);

-- ----------------------------------------------------------------------
-- 1분 윈도우 × 아이템 집계. events 에서 다시 계산해 upsert 하므로 몇 번 돌려도 값이 같다
-- ----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS item_stats_1m (
    window_start     TIMESTAMPTZ NOT NULL,             -- date_trunc('minute', event_time)
    item_id          BIGINT      NOT NULL,
    view_cnt         INT         NOT NULL DEFAULT 0,
    addtocart_cnt    INT         NOT NULL DEFAULT 0,
    transaction_cnt  INT         NOT NULL DEFAULT 0,
    unique_visitors  INT         NOT NULL DEFAULT 0,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, item_id)
);
CREATE INDEX IF NOT EXISTS idx_item_stats_item ON item_stats_1m (item_id, window_start);

-- ----------------------------------------------------------------------
-- 일 배치(Airflow) 결과. Parquet 전체 이력에서 계산하므로 30일보다 오래된 날짜도 채울 수 있다
-- ----------------------------------------------------------------------
-- 하루 × 상품 집계. DAG 가 그 날짜 행을 DELETE 후 INSERT (한 트랜잭션) → 다시 돌려도 같다
CREATE TABLE IF NOT EXISTS item_stats_daily (
    dt               DATE        NOT NULL,             -- 이벤트 날짜 (UTC)
    item_id          BIGINT      NOT NULL,
    view_cnt         INT         NOT NULL,
    addtocart_cnt    INT         NOT NULL,
    transaction_cnt  INT         NOT NULL,
    unique_visitors  INT         NOT NULL,
    PRIMARY KEY (dt, item_id)
);

-- 대시보드용 하루 요약 1행. 품질 검사를 통과한 날짜만 PK(dt) 로 업서트
CREATE TABLE IF NOT EXISTS daily_summary (
    dt               DATE        PRIMARY KEY,
    events           INT         NOT NULL,
    view_cnt         INT         NOT NULL,
    addtocart_cnt    INT         NOT NULL,
    transaction_cnt  INT         NOT NULL,
    unique_visitors  INT         NOT NULL,
    unique_items     INT         NOT NULL,
    cart_rate        NUMERIC(8,6),                     -- addtocart / view
    purchase_rate    NUMERIC(8,6),                     -- transaction / view
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
