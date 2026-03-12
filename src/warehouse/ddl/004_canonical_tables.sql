-- ============================================================================
-- Canonical Business Metrics Tables
-- ============================================================================
USE DATABASE CHATGPT_PLATFORM;
USE SCHEMA CANONICAL;

CREATE TABLE IF NOT EXISTS DAILY_ACTIVE_USERS (
    event_date         DATE NOT NULL,
    total_dau          INTEGER,
    dau_free           INTEGER,
    dau_plus           INTEGER,
    dau_enterprise     INTEGER,
    dau_web            INTEGER,
    dau_mobile         INTEGER,
    dau_api            INTEGER,
    new_users_today    INTEGER,
    returning_users    INTEGER,
    _computed_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS WEEKLY_ENGAGEMENT (
    week_start         DATE NOT NULL,
    user_tier          VARCHAR(20),
    total_sessions     BIGINT,
    total_messages     BIGINT,
    avg_tokens_per_message FLOAT,
    avg_messages_per_session FLOAT,
    p50_latency_ms     FLOAT,
    p95_latency_ms     FLOAT,
    p99_latency_ms     FLOAT,
    unique_models_used INTEGER,
    _computed_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS MONTHLY_REVENUE (
    month_start        DATE NOT NULL,
    total_revenue_usd  FLOAT,
    subscription_revenue_usd FLOAT,
    api_usage_revenue_usd FLOAT,
    new_subscriptions  INTEGER,
    churned_subscriptions INTEGER,
    upgrades           INTEGER,
    downgrades         INTEGER,
    refunds_usd        FLOAT,
    mrr_usd            FLOAT,
    _computed_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
