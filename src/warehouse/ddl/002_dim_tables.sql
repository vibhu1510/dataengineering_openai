-- ============================================================================
-- Dimension Tables
-- ============================================================================
USE DATABASE CHATGPT_PLATFORM;
USE SCHEMA WAREHOUSE;

-- dim_users: SCD Type 2 — tracks changes to user tier, status, org
CREATE TABLE IF NOT EXISTS DIM_USERS (
    user_sk           INTEGER AUTOINCREMENT PRIMARY KEY,
    user_id           VARCHAR(64) NOT NULL,
    email_hash        VARCHAR(64),
    display_name      VARCHAR(256),
    signup_date       TIMESTAMP_NTZ,
    country_code      VARCHAR(2),
    user_tier         VARCHAR(20),            -- free, plus, enterprise
    organization_id   VARCHAR(64),
    is_api_user       BOOLEAN DEFAULT FALSE,
    account_status    VARCHAR(20) DEFAULT 'active',  -- active, suspended, quarantined
    valid_from        TIMESTAMP_NTZ NOT NULL,
    valid_to          TIMESTAMP_NTZ,
    is_current        BOOLEAN DEFAULT TRUE
);

-- dim_models: one row per model version
CREATE TABLE IF NOT EXISTS DIM_MODELS (
    model_sk          INTEGER AUTOINCREMENT PRIMARY KEY,
    model_id          VARCHAR(64) NOT NULL,
    model_family      VARCHAR(32),             -- gpt-4, gpt-3.5, o1
    model_version     VARCHAR(32),
    context_window    INTEGER,
    max_output_tokens INTEGER,
    cost_per_1k_input  FLOAT,
    cost_per_1k_output FLOAT,
    release_date      DATE,
    is_deprecated     BOOLEAN DEFAULT FALSE
);

-- dim_time: pre-populated calendar dimension
CREATE TABLE IF NOT EXISTS DIM_TIME (
    date_key          INTEGER PRIMARY KEY,      -- YYYYMMDD
    full_date         DATE NOT NULL,
    day_of_week       INTEGER,
    day_name          VARCHAR(10),
    week_of_year      INTEGER,
    month_num         INTEGER,
    month_name        VARCHAR(10),
    quarter           INTEGER,
    year              INTEGER,
    is_weekend        BOOLEAN,
    is_us_holiday     BOOLEAN DEFAULT FALSE,
    fiscal_quarter    INTEGER,
    fiscal_year       INTEGER
);

-- dim_subscription_plans
CREATE TABLE IF NOT EXISTS DIM_SUBSCRIPTION_PLANS (
    plan_sk           INTEGER AUTOINCREMENT PRIMARY KEY,
    plan_id           VARCHAR(32) NOT NULL,
    plan_name         VARCHAR(64),
    monthly_price_usd FLOAT,
    annual_price_usd  FLOAT,
    rate_limit_rpm    INTEGER,
    max_tokens_per_day INTEGER,
    models_available  VARIANT,                  -- JSON array of model_ids
    valid_from        DATE,
    valid_to          DATE
);
