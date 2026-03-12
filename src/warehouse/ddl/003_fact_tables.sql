-- ============================================================================
-- Fact Tables (star schema)
-- ============================================================================
USE DATABASE CHATGPT_PLATFORM;
USE SCHEMA WAREHOUSE;

-- fact_conversations: grain = one message exchange
CREATE TABLE IF NOT EXISTS FACT_CONVERSATIONS (
    conversation_fact_id BIGINT AUTOINCREMENT PRIMARY KEY,
    event_id           VARCHAR(64) NOT NULL,
    event_type         VARCHAR(32),
    event_timestamp    TIMESTAMP_NTZ NOT NULL,
    date_key           INTEGER REFERENCES DIM_TIME(date_key),
    user_sk            INTEGER REFERENCES DIM_USERS(user_sk),
    model_sk           INTEGER REFERENCES DIM_MODELS(model_sk),
    session_id         VARCHAR(64),
    conversation_id    VARCHAR(64),
    computed_session_id VARCHAR(80),
    message_role       VARCHAR(16),
    input_tokens       INTEGER,
    output_tokens      INTEGER,
    total_tokens       INTEGER,
    latency_ms         INTEGER,
    rating             SMALLINT,
    is_streaming       BOOLEAN,
    has_code_block     BOOLEAN,
    platform           VARCHAR(16),
    country_code       VARCHAR(2),
    _loaded_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- fact_api_usage: grain = one API call
CREATE TABLE IF NOT EXISTS FACT_API_USAGE (
    api_usage_fact_id  BIGINT AUTOINCREMENT PRIMARY KEY,
    event_id           VARCHAR(64) NOT NULL,
    event_timestamp    TIMESTAMP_NTZ NOT NULL,
    date_key           INTEGER,
    user_sk            INTEGER,
    model_sk           INTEGER,
    api_key_hash       VARCHAR(64),
    endpoint           VARCHAR(128),
    http_status_code   INTEGER,
    response_category  VARCHAR(20),
    input_tokens       INTEGER,
    output_tokens      INTEGER,
    total_tokens       INTEGER,
    latency_ms         INTEGER,
    is_streaming       BOOLEAN,
    is_rate_limited    BOOLEAN,
    organization_id    VARCHAR(64),
    estimated_cost_usd FLOAT,
    _loaded_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- fact_billing: grain = one billing event
CREATE TABLE IF NOT EXISTS FACT_BILLING (
    billing_fact_id    BIGINT AUTOINCREMENT PRIMARY KEY,
    event_id           VARCHAR(64) NOT NULL,
    event_type         VARCHAR(32),
    event_timestamp    TIMESTAMP_NTZ NOT NULL,
    date_key           INTEGER,
    user_sk            INTEGER,
    plan_sk            INTEGER,
    amount_usd         FLOAT,
    currency           VARCHAR(3),
    payment_method     VARCHAR(32),
    payment_status     VARCHAR(20),
    billing_period_start DATE,
    billing_period_end   DATE,
    is_trial           BOOLEAN DEFAULT FALSE,
    revenue_impact     VARCHAR(20),
    requires_followup  BOOLEAN DEFAULT FALSE,
    _loaded_at         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
