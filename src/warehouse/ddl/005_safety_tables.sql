-- ============================================================================
-- Trust & Safety Tables
-- ============================================================================
USE DATABASE CHATGPT_PLATFORM;
USE SCHEMA SAFETY;

CREATE TABLE IF NOT EXISTS SAFETY_DASHBOARD (
    event_date                  DATE NOT NULL,
    total_safety_signals        INTEGER,
    prompt_injection_attempts   INTEGER,
    jailbreak_attempts          INTEGER,
    rate_limit_violations       INTEGER,
    content_policy_flags        INTEGER,
    accounts_quarantined        INTEGER,
    accounts_released           INTEGER,
    false_positive_rate         FLOAT,
    mean_time_to_detect_seconds FLOAT,
    _computed_at                TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS QUARANTINED_ACCOUNTS (
    user_id            VARCHAR(64) NOT NULL,
    quarantine_reason  VARCHAR(256),
    violation_type     VARCHAR(64),
    severity           VARCHAR(20),
    violation_count    INTEGER,
    confidence_score   FLOAT,
    quarantined_at     TIMESTAMP_NTZ NOT NULL,
    reviewed_at        TIMESTAMP_NTZ,
    review_outcome     VARCHAR(32),   -- confirmed_abuse, false_positive, released
    is_active          BOOLEAN DEFAULT TRUE
);
