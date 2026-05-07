-- airflow_db 생성
CREATE DATABASE airflow_db;

-- mart_funnel_daily
CREATE TABLE IF NOT EXISTS mart_funnel_daily (
    dt               DATE         NOT NULL,
    click_count      BIGINT       DEFAULT 0,
    cart_count       BIGINT       DEFAULT 0,
    order_count      BIGINT       DEFAULT 0,
    click_to_cart    NUMERIC(5,2) DEFAULT 0,
    cart_to_order    NUMERIC(5,2) DEFAULT 0,
    click_to_order   NUMERIC(5,2) DEFAULT 0,
    created_at       TIMESTAMP    DEFAULT NOW(),
    PRIMARY KEY (dt)
);

-- mart_session
CREATE TABLE IF NOT EXISTS mart_session (
    session_id       BIGINT       NOT NULL,
    user_id          VARCHAR(64)  NOT NULL,
    session_start    TIMESTAMP,
    session_end      TIMESTAMP,
    duration_sec     BIGINT,
    event_count      BIGINT       DEFAULT 0,
    click_count      BIGINT       DEFAULT 0,
    cart_count       BIGINT       DEFAULT 0,
    order_count      BIGINT       DEFAULT 0,
    platform         VARCHAR(10),
    region           VARCHAR(20),
    created_at       TIMESTAMP    DEFAULT NOW(),
    PRIMARY KEY (session_id)
);

CREATE INDEX IF NOT EXISTS idx_mart_session_user_id ON mart_session (user_id);

-- mart_cohort
CREATE TABLE IF NOT EXISTS mart_cohort (
    cohort_month     VARCHAR(7)   NOT NULL,
    activity_month   VARCHAR(7)   NOT NULL,
    cohort_size      BIGINT       DEFAULT 0,
    active_users     BIGINT       DEFAULT 0,
    retention_rate   NUMERIC(5,2) DEFAULT 0,
    created_at       TIMESTAMP    DEFAULT NOW(),
    PRIMARY KEY (cohort_month, activity_month)
);

-- mart_item_stats
CREATE TABLE IF NOT EXISTS mart_item_stats (
    aid              BIGINT       NOT NULL,
    click_count      BIGINT       DEFAULT 0,
    cart_count       BIGINT       DEFAULT 0,
    order_count      BIGINT       DEFAULT 0,
    cart_rate        NUMERIC(5,2) DEFAULT 0,
    order_rate       NUMERIC(5,2) DEFAULT 0,
    created_at       TIMESTAMP    DEFAULT NOW(),
    PRIMARY KEY (aid)
);

-- stg_events
CREATE TABLE IF NOT EXISTS stg_events (
    session_id       BIGINT       NOT NULL,
    aid              BIGINT       NOT NULL,
    ts               BIGINT       NOT NULL,
    event_dt         TIMESTAMP,
    event_date       DATE,
    event_type       VARCHAR(10)  NOT NULL,
    user_id          VARCHAR(64)  NOT NULL,
    gender           VARCHAR(1),
    age_group        VARCHAR(10),
    region           VARCHAR(20),
    membership       VARCHAR(10),
    platform         VARCHAR(10),
    created_at       TIMESTAMP    DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_stg_events_user_id    ON stg_events (user_id);
CREATE INDEX IF NOT EXISTS idx_stg_events_event_type ON stg_events (event_type);
CREATE INDEX IF NOT EXISTS idx_stg_events_event_date ON stg_events (event_date);