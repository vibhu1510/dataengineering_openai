# ChatGPT Data Platform

An end-to-end data engineering platform simulating OpenAI's ChatGPT data infrastructure. Covers the full lifecycle: event ingestion, stream processing, batch ETL, dimensional modeling, canonical metric generation, trust & safety systems, data quality enforcement, and operational monitoring.

Built to mirror the real challenges of running a platform at scale: late-arriving data, schema evolution, PII compliance, abuse detection, SCD tracking, and balancing throughput against correctness.

## Architecture

```
┌─────────────────────┐
│  Synthetic Data      │
│  Generators          │
│  (Python + Faker)    │
│  5 event types       │
│  weighted traffic    │
└────────┬────────────┘
         │ JSON events (Kafka producer, acks=all)
         ▼
┌─────────────────────┐     ┌─────────────────────┐
│  Apache Kafka        │────▶│  Faust Consumers     │
│  5 topics:           │     │  (async agents)      │
│  • user_events       │     │  1 agent per topic   │
│  • conversation_events│    └────────┬────────────┘
│  • api_usage_events  │             │ Micro-batched Parquet
│  • billing_events    │             │ (Snappy, 1000-event flush)
│  • safety_events     │             ▼
└──────────────────────┘    ┌─────────────────────┐
                            │  MinIO (S3)          │
                            │  Hive-partitioned:   │
                            │  {topic}/dt=YYYY-MM-DD/hr=HH │
                            └────────┬────────────┘
                                     │
                  ┌──────────────────┤
                  │ Airflow DAGs     │
                  ▼                  ▼
        ┌─────────────────┐  ┌──────────────────┐
        │  PySpark ETL     │  │  Safety Pipeline  │
        │  • Dedup         │  │  • Z-score anomaly│
        │  • Sessionize    │  │  • Injection scan │
        │  • SCD Type 2    │  │  • Rate detection │
        │  • PII masking   │  │  • Quarantine mgr │
        │  • Engagement    │  │  • Data retention │
        │    scoring       │  └──────────────────┘
        └────────┬────────┘
                 │
                 ▼
        ┌─────────────────────────────────┐
        │  Snowflake Data Warehouse        │
        │                                  │
        │  6 schemas: RAW, STAGING,        │
        │  WAREHOUSE, CANONICAL, SAFETY,   │
        │  EXPORT                          │
        │                                  │
        │  WAREHOUSE (star schema)         │
        │  ├── dim_users (SCD2)            │
        │  ├── dim_models (with pricing)   │
        │  ├── dim_time (calendar)         │
        │  ├── dim_subscription_plans      │
        │  ├── fact_conversations          │
        │  ├── fact_api_usage              │
        │  └── fact_billing                │
        │                                  │
        │  CANONICAL (business metrics)    │
        │  ├── daily_active_users          │
        │  ├── weekly_engagement           │
        │  └── monthly_revenue             │
        │                                  │
        │  SAFETY                          │
        │  ├── safety_dashboard            │
        │  └── quarantined_accounts        │
        └────────────┬────────────────────┘
                     │
                     ▼
        ┌─────────────────────┐
        │  Streamlit Dashboard │
        │  4 tabs:             │
        │  • Pipeline Health   │
        │  • Data Quality      │
        │  • Safety Monitor    │
        │  • Business Metrics  │
        └─────────────────────┘
```

## Tech Stack

| Component          | Technology                    | Purpose                              | Why This Choice |
|-------------------|-------------------------------|--------------------------------------|-----------------|
| Orchestration     | Apache Airflow 2.8            | DAG scheduling, monitoring, retries  | Industry standard, rich operator library, native sensor support for data-aware scheduling |
| Stream Processing | Kafka + Faust-streaming       | Real-time event ingestion            | Kafka provides durable pub/sub with ordering guarantees; Faust adds Python-native async consumers without JVM overhead |
| Batch Processing  | PySpark 3.5                   | ETL transformations at scale         | Window functions, UDFs, native Parquet/S3/Snowflake connectors; same API works from local mode to multi-node clusters |
| Data Warehouse    | Snowflake                     | Star schema, canonical metrics       | Separation of storage and compute, semi-structured data support (VARIANT), zero-maintenance scaling |
| Object Storage    | MinIO (S3-compatible)         | Staging area for Parquet files       | Drop-in S3 replacement for local dev; same S3A connector code works against production AWS |
| Data Quality      | Custom rule engine            | Pre/post-load validation             | Lightweight, no external dependencies; seven check types covering schema, nulls, uniqueness, ranges, regex |
| Dashboard         | Streamlit + Plotly            | Monitoring and visualization         | Rapid prototyping for data apps; Plotly for interactive charts |
| Infrastructure    | Docker Compose                | Local development environment        | Single-command startup for 7 interconnected services |

## Quick Start

```bash
# 1. Clone and configure
cp .env.example .env
# Edit .env with your Snowflake credentials

# 2. Start all services
make up

# 3. Run the full demo
make demo
```

**Service URLs:**
- Airflow UI: http://localhost:8080 (admin/admin)
- MinIO Console: http://localhost:9003 (minioadmin/minioadmin)
- Streamlit Dashboard: http://localhost:8501 (after `make dashboard`)

## Project Structure

```
├── dags/                          # Airflow DAGs
│   ├── daily_batch_etl.py         # Main daily pipeline (6-stage, TaskGroups)
│   ├── hourly_safety_monitoring.py
│   └── common/                    # MinIOFileSensor, alert callbacks
├── src/
│   ├── config/                    # Central config (frozen dataclasses, 12-factor)
│   ├── generators/                # Synthetic data generators (5 event types)
│   ├── streaming/                 # Faust agents + MinIO Parquet sink
│   ├── spark_jobs/
│   │   ├── common/                # Spark session factory, Snowflake I/O helpers
│   │   ├── transformations/       # Dedup, sessionization, SCD2, PII masking, engagement scoring
│   │   └── jobs/                  # 4 ETL jobs + 4 canonical metric builders
│   ├── warehouse/ddl/             # Snowflake DDL (5 scripts, 6 schemas)
│   ├── safety/                    # Anomaly detection, prompt injection, quarantine, retention
│   ├── quality/                   # Staging + warehouse data quality validators
│   └── dashboard/                 # Streamlit monitoring app (4 tabs)
├── tests/                         # Unit (Spark transforms, generators, safety) + DAG integrity
├── docker-compose.yml             # Full infrastructure stack (7 services)
└── Makefile                       # Common commands
```

---

## Design Decisions & Real-World Rationale

### 1. Event-Driven Ingestion (Kafka + Faust)

**What it does:** Five Kafka topics receive JSON events from synthetic generators. Faust async agents consume each topic independently and write micro-batched Parquet files to MinIO.

**Why this design:**
- **Decoupling producers from consumers** is the core Kafka value proposition. Generators can publish at any rate without blocking downstream processing. In a real ChatGPT platform, API servers emit events into Kafka and never wait for the warehouse.
- **One agent per topic** with independent buffer state means a spike in billing events does not delay conversation event processing. This mirrors how production systems isolate failure domains.
- **Micro-batch Parquet (1000-event threshold + 60-second timer)** balances two competing concerns: frequent flushes keep latency low, but flushing too often creates thousands of small files that degrade Spark read performance. The dual trigger (count OR time) ensures data lands within a minute even during low-traffic periods.

**Trade-offs acknowledged:**
- The in-memory event buffer has **no write-ahead log**. A crash loses up to 999 events. In production, you would add Kafka consumer offset management with at-least-once semantics and dedup downstream (which this pipeline does in the Spark layer). The trade-off here is simplicity vs. durability.
- Faust + aiokafka version compatibility is fragile. Production systems often use Kafka Connect or Spark Structured Streaming for the same role, trading Python simplicity for JVM maturity.

**Real-world parallel:** This is how OpenAI's actual data infrastructure likely works: API servers publish to Kafka, stream processors land events into object storage, and batch jobs pick up from there.

---

### 2. Hive-Partitioned Staging (MinIO as S3)

**What it does:** Parquet files are written to MinIO with paths like `conversation_events/dt=2026-03-11/hr=14/batch_1710...parquet`, enabling Spark to read with partition pruning.

**Why this design:**
- **Partition pruning** means `spark.read.parquet("s3a://staging/conversation_events/").filter(col("dt") == "2026-03-11")` only reads files from that date's directory, not the entire topic history. On a real platform with billions of events, this is the difference between a 30-second job and a 30-minute job.
- **Hive-style partitioning (`dt=`/`hr=`)** is the universally supported convention across Spark, Hive, Presto, Athena, and Trino. Any query engine can read this layout without configuration.
- **Snappy compression** is the standard Parquet codec: fast decompression speed with good compression ratio. The alternative (Zstd) compresses ~20% better but decompresses slower, which matters when Spark tasks are CPU-bound on deserialization.

**Trade-off:** Hourly partitioning creates 24 directories per day per topic. With 5 topics, that's 120 directories/day. Over a year, listing objects becomes expensive. Production systems add periodic compaction jobs to merge small files within partitions.

---

### 3. PySpark Transformations

#### Deduplication (`src/spark_jobs/transformations/deduplication.py`)

**What it does:** Uses `row_number()` over a window partitioned by `event_id`, ordered by timestamp, to keep exactly one copy of each event.

**Why `row_number()` over `dropDuplicates()`:**
- `dropDuplicates()` is non-deterministic about which row it keeps. `row_number()` with explicit ordering guarantees we keep the latest (or earliest) record, which matters when the same event arrives multiple times with different metadata.
- Window functions preserve all columns without listing them explicitly, making the transform schema-agnostic.

**Real-world context:** At-least-once delivery from Kafka guarantees duplicates will exist. Every production data pipeline needs a dedup step. The event_id + timestamp pattern is standard practice.

#### Sessionization (`src/spark_jobs/transformations/sessionization.py`)

**What it does:** Groups user events into sessions using gap-based detection: if consecutive events from the same user are more than 30 minutes apart, a new session starts. Uses `lag()` for gap computation and cumulative sum for session numbering.

**Why gap-based over fixed windows:**
- Fixed time windows (e.g., hourly buckets) split active sessions at arbitrary boundaries. Gap-based sessionization creates sessions that reflect actual user behavior patterns.
- The 30-minute default matches industry convention (Google Analytics uses the same threshold). The parameter is configurable.

**Trade-off:** Gap-based sessionization assumes events arrive in timestamp order within each partition. Late-arriving events that fall within a previously closed session will start a new session instead of being appended. Production systems handle this with watermarking and session windows in stream processing, but for batch ETL on already-landed data, this is acceptable.

#### SCD Type 2 (`src/spark_jobs/transformations/scd_type2.py`)

**What it does:** Implements full slowly-changing-dimension merge logic for tracking historical changes to user attributes (tier, status, country, organization). Detects changes via MD5 hash comparison, expires old rows by setting `valid_to` and `is_current=False`, and inserts new versions.

**Why MD5 hash comparison over column-by-column diff:**
- Comparing N columns individually requires N equality checks per row. MD5 hashing concatenates all tracked columns into a single hash, reducing change detection to one comparison regardless of how many columns are tracked.
- Null handling uses a sentinel value (`__NULL__`) before hashing, preventing `NULL != NULL` from triggering false change detections.

**Why five-way union over MERGE:**
- The transform splits into five explicit DataFrame operations (unchanged, expired, new versions, new records, untouched history) and unions them. This is more verbose than a SQL MERGE but makes each logical case independently testable and debuggable.
- In PySpark there is no native MERGE statement. The alternative would be writing to a temporary table and using Snowflake's MERGE, but that pushes transformation logic out of Spark.

**Trade-off:** Five separate joins all scan both DataFrames, meaning more shuffles. A single-pass approach with `cogroup` would be more efficient but significantly harder to read and maintain. For dimension tables (typically millions of rows, not billions), the performance difference is negligible.

**Real-world context:** SCD Type 2 is how every data warehouse tracks dimension history. When a ChatGPT user upgrades from Free to Plus, you need to know they were Free when they had certain conversations and Plus for later ones. This is critical for accurate revenue attribution and engagement analysis.

#### PII Masking (`src/spark_jobs/transformations/pii_masking.py`)

**What it does:** Applies regex-based redaction for emails, phone numbers, SSNs, and IP addresses in free-text columns, replacing matches with descriptive placeholders like `[EMAIL_REDACTED]`.

**Why regex UDFs over native Spark functions:**
- Applying four regex patterns sequentially with `regexp_replace` is possible in native Spark, but the PII masking logic needs to be a single composable function that can be applied to any column, and the patterns share a common application flow.
- The UDF approach keeps all masking logic in one function, making it easier to audit for compliance reviews.

**Trade-off:** Python UDFs serialize data between the JVM and Python, which is 10-100x slower than native Spark expressions. For a production pipeline processing billions of messages, you would rewrite this as chained `regexp_replace` calls or use a Pandas UDF (vectorized) for 10x improvement. The current approach prioritizes readability and auditability over raw throughput.

**Real-world context:** GDPR, CCPA, and SOC 2 all require PII to be masked or tokenized before entering analytical systems. This is non-negotiable for any platform handling user data.

#### Engagement Scoring (`src/spark_jobs/transformations/engagement_scoring.py`)

**What it does:** Computes a composite engagement score (0-1) per user per day, weighting session frequency (30%), message volume (25%), API activity (25%), and model diversity (20%). Uses a full outer join of conversation and API metrics so single-channel users are still scored.

**Why a composite score:**
- Individual metrics (sessions, messages, API calls) each tell part of the story. A single score enables ranking, cohort segmentation, and threshold-based alerting without requiring downstream consumers to understand the component metrics.
- The weights are tunable parameters, not hardcoded business logic. A data science team would calibrate these against retention data.

**Trade-off:** Heuristic scoring (linear normalization, hand-tuned weights) is a starting point. Production systems would use ML-based scoring trained on retention/churn labels. The heuristic approach ships faster and is interpretable, which matters when building trust with stakeholders.

---

### 4. Star Schema Data Model

**What it does:** Four dimension tables + three fact tables following Kimball methodology, with surrogate keys, grain documentation, and audit columns.

| Table | Grain | Key Design Choice |
|-------|-------|-------------------|
| `dim_users` | One row per user per version (SCD2) | `valid_from/valid_to/is_current` enables point-in-time queries |
| `dim_models` | One row per AI model | Includes `cost_per_1k_input/output` for cost analysis directly from the dimension |
| `dim_time` | One row per calendar day | Pre-populated with fiscal quarters, US holidays, weekend flags |
| `dim_subscription_plans` | One row per plan version | Uses Snowflake VARIANT for `models_available` JSON array |
| `fact_conversations` | One message exchange | Includes `computed_session_id` linking to sessionization output |
| `fact_api_usage` | One API call | `estimated_cost_usd` computed during ETL using model pricing |
| `fact_billing` | One billing event | `revenue_impact` classification (positive/negative/neutral) |

**Why star schema over normalized/data vault:**
- Star schema is optimized for analytical queries. A single JOIN from fact to dimension answers most business questions. Data vault (hub/link/satellite) is better for auditability and late-binding business rules, but adds query complexity.
- Snowflake's columnar storage and automatic micro-partitioning make star schema queries very efficient. The JOIN overhead that hurts star schemas on row-oriented databases is minimal on Snowflake.

**Why surrogate keys (AUTOINCREMENT) over natural keys:**
- Natural keys (like `user_id`) change meaning over time with SCD2. A surrogate key uniquely identifies a specific version of a dimension record.
- Foreign key references in facts point to the surrogate key, enabling point-in-time joins: "what tier was this user on when this conversation happened?"

**Trade-off:** Snowflake foreign key constraints are informational only (not enforced). Referential integrity depends entirely on the ETL pipeline's correctness. The post-load quality checks partially mitigate this, but there is no hard database-level guarantee.

---

### 5. Canonical Business Metrics

Three canonical tables serve as the "single source of truth" for key business questions:

**Daily Active Users** (`build_canonical_dau.py`):
- Unions activity from conversations AND API usage to avoid undercounting users who only use the API
- Segments by tier (free/plus/enterprise), platform (web/mobile/api), and new vs. returning
- Identifies new users by matching `signup_date` to execution date

**Weekly Engagement** (`build_canonical_engagement.py`):
- Monday-to-Sunday week boundaries for consistent reporting
- Uses `percentile_approx` for latency percentiles (p50/p95/p99) because exact percentiles require sorting the entire dataset, while approximate percentiles compute in a single pass with bounded error
- Model diversity metric shows whether users explore multiple AI models or stick to one

**Monthly Revenue** (`build_canonical_revenue.py`):
- Single-pass conditional aggregation: `sum(when(event_type == "payment", amount))` computes subscription and API revenue simultaneously without multiple table scans
- Tracks subscription lifecycle: new subscriptions, upgrades, downgrades, churns, refunds
- Simplified MRR (Monthly Recurring Revenue) calculation for trend analysis

**Why canonical tables matter:**
At a company like OpenAI, dozens of teams (product, finance, growth, safety) all need DAU, engagement, and revenue numbers. Without canonical tables, each team writes their own query with slightly different logic, producing conflicting numbers. Canonical tables enforce one definition of "active user" and one definition of "revenue" across the entire organization.

---

### 6. Airflow Orchestration

**Daily Batch ETL** (`dags/daily_batch_etl.py`):
```
sensors → pre_load_quality → dimension_loads → fact_loads → post_load_quality → canonical_builds
```

Key design decisions:
- **TaskGroups** organize 15+ tasks into 6 logical stages, keeping the Airflow graph readable
- **Sensor mode = "reschedule"** instead of "poke": frees up worker slots between checks rather than blocking a worker thread sleeping. Critical for resource efficiency when sensors wait 10+ minutes.
- **Dimensions before facts**: Enforces referential integrity. If `dim_users` fails, fact loads are skipped rather than writing facts with missing dimension keys.
- **Quality gates at pipeline boundaries**: Pre-load checks validate staging data before any warehouse writes. Post-load checks validate row counts and referential integrity after loading. Either gate failing blocks all downstream tasks.
- **`max_active_runs=1`** prevents overlapping daily runs, which could cause data corruption in SCD2 merges
- **`catchup=False`** prevents historical backfill runs when the DAG is first deployed or re-enabled

**Failure handling:**
- 3 retries with exponential backoff (5-minute base, 30-minute cap)
- 2-hour SLA with a dedicated miss callback
- Structured failure callbacks with task context for alerting

**Hourly Safety Monitoring** (`dags/hourly_safety_monitoring.py`):
```
detect_anomalies → update_quarantine → refresh_safety_dashboard
```
Runs independently from the daily ETL on an hourly schedule, because safety response time matters more than waiting for the daily batch.

**Custom MinIO Sensor** (`dags/common/sensors.py`):
- Polls MinIO for file existence at a given prefix with configurable `min_objects` threshold
- `template_fields = ("prefix",)` enables Jinja templating for date-parameterized paths: `dt={{ ds }}`

**Trade-off:** Spark jobs are invoked via `BashOperator` with `spark-submit --master local[*]`. This is simpler than SparkSubmitOperator or KubernetesPodOperator, but provides less structured error reporting. In production, you would use a KubernetesPodOperator or ECS task for isolated resource management and better failure handling.

---

### 7. Trust & Safety Pipeline

**Why safety is a separate pipeline, not embedded in ETL:**
Safety monitoring needs faster response times than daily batch ETL. A prompt injection attack should trigger quarantine within an hour, not 24 hours. The hourly DAG decouples safety response time from data warehouse freshness.

#### Anomaly Detection (`src/safety/detectors/anomaly_detector.py`)
- **Z-score method** (threshold: 3.0 = 99.7th percentile) flags users whose violation counts deviate significantly from their own historical baseline
- **Minimum sample size guard** (N >= 5) prevents false positives on new users with limited history
- **Rate anomaly detection** compares per-user hourly API request counts against global mean + 5x stddev, catching automated abuse patterns

**Trade-off:** Statistical methods (Z-score) are simple and interpretable but miss sophisticated attack patterns. Production safety systems use ML models trained on labeled abuse data. Statistical detection is the right starting point because it requires no training data and provides a baseline that ML models can improve upon.

#### Prompt Injection Detection (`src/safety/detectors/prompt_injection_detector.py`)
- **14 regex patterns** covering 7 attack categories: instruction override, role manipulation, system prompt extraction, delimiter attacks, jailbreak attempts, filter bypass, and encoding attacks
- **Weighted scoring** (0.5-0.95 per pattern) reflects that some patterns are stronger indicators than others. "Ignore all previous instructions" (0.9) is a stronger signal than a generic delimiter attack (0.5).
- **Configurable threshold** (default 0.5) for the binary `is_injection_attempt` flag

**Trade-off:** Pattern matching catches known attack signatures but misses novel techniques. Production systems layer regex detection with embedding-based classifiers and LLM-as-judge evaluations. The regex approach is fast (sub-millisecond per message) and has zero false positives on the patterns it covers, making it a reliable first-pass filter.

#### Account Quarantine (`src/safety/detectors/quarantine_manager.py`)
- **Two-tier threshold**: 5+ high-severity violations OR 1 critical violation with confidence >= 0.7
- **Human-in-the-loop**: Quarantine records include `reviewed_at` and `review_outcome` fields, establishing that automated quarantine is a holding action, not a final decision
- **Append-only audit trail**: Every quarantine action is logged for compliance review

**Real-world context:** This mirrors how platforms like OpenAI handle trust & safety. Automated systems flag and quarantine suspicious accounts. Human reviewers then investigate and either confirm the ban or release the account. The two-tier threshold ensures that a single critical violation (e.g., a confirmed prompt injection with high confidence) triggers immediate action without waiting for a pattern to develop.

#### Data Retention (`src/safety/compliance/data_retention.py`)
- **Per-table retention policies** ranging from 90 days (raw safety events) to 7 years (billing records, per financial regulations)
- **Audit logging** after every deletion: what was deleted, how many rows, when, from which table

**Trade-off:** The implementation uses direct SQL DELETEs against Snowflake, which is efficient for periodic cleanup but can be expensive for very large tables. Production systems use Snowflake's time-travel and fail-safe features for soft deletes, or partition-based drops for efficient bulk removal.

---

### 8. Data Quality Framework

**Pre-load validation** (`src/quality/expectations/validate_staging.py`):
Seven check types applied to staging data before any warehouse write:

| Check | What It Validates | Why It Matters |
|-------|-------------------|----------------|
| `not_empty` | DataFrame has > 0 rows | Prevents writing empty partitions that mask missing data |
| `column_exists` | Required columns are present | Catches schema drift from upstream changes |
| `no_nulls` | Critical columns have no NULLs | Prevents NULL foreign keys in fact tables |
| `unique` | Event IDs are unique | Validates deduplication was effective |
| `values_in_set` | Enum columns contain only valid values | Catches data corruption or new unhandled event types |
| `column_between` | Numeric columns are in expected ranges | Catches unit errors (e.g., negative token counts) |
| `column_matches_regex` | String columns match expected patterns | Validates ID formats (e.g., `usr_[a-f0-9]{16}`) |

**Post-load validation** (`src/quality/expectations/validate_warehouse.py`):
- Row count comparison between staging input and warehouse output
- Current-row counts for SCD2 dimensions (every user should have exactly one `is_current=True` row)
- Referential integrity spot-checks (fact table foreign keys exist in dimension tables)

**Why custom over Great Expectations:**
The custom framework is ~200 lines with zero dependencies. Great Expectations brings a rich UI and checkpoint system but adds complexity in Docker images and configuration. For a pipeline with well-defined quality rules, a lightweight custom engine provides the same enforcement guarantees with faster iteration.

---

### 9. Synthetic Data Generation

**Why synthetic data matters:** Real ChatGPT data is proprietary and PII-laden. Synthetic generators produce data with the same statistical properties (distributions, correlations, edge cases) without privacy concerns, enabling full pipeline testing.

**Realistic modeling:**
- **Tier-based model access**: Free users get GPT-3.5 only; Plus users get GPT-4; Enterprise users get all models. This mirrors real platform access controls and affects engagement/revenue metrics.
- **Weighted event distribution**: 40% conversations, 25% API, 15% user events, 10% billing, 10% safety. This reflects that conversations are the primary user activity.
- **Lognormal latency**: Response times follow `lognormvariate(6.5, 0.8)` capped at 60s, producing the long-tail distribution seen in real API latency.
- **Correlated session behavior**: Sessions generate 1-15 messages with a weighted distribution skewed toward shorter conversations. 30% of sessions get rated; 20% of rated sessions have text feedback. These ratios match typical user engagement patterns.

**Two generation modes:**
- **Stream mode**: Real-time publishing to Kafka with rate pacing (configurable events/second). Tests the full streaming path.
- **Batch mode**: Generates historical data as JSONL files. Useful for backfilling staging and testing Spark jobs independently.

---

### 10. Configuration & Infrastructure

**12-Factor Configuration** (`src/config/settings.py`):
- Frozen dataclasses make config immutable after initialization, preventing accidental mutation during pipeline execution
- All values read from environment variables with sensible defaults for local development
- `connection_params` properties centralize connection string construction for Spark, Snowflake, and MinIO

**Docker Compose** (7 services):
- **Zookeeper + Kafka**: Message broker with ordering guarantees
- **MinIO**: S3-compatible object storage (ports remapped to 9002/9003 to avoid Docker Desktop conflicts)
- **PostgreSQL**: Airflow metadata database
- **Airflow webserver + scheduler**: DAG execution and monitoring
- **Faust worker**: Stream processing consumers

**Trade-off:** Docker Compose is single-machine. Production deployments would use Kubernetes with Helm charts, Kafka on Confluent Cloud or MSK, and S3 instead of MinIO. The Docker Compose setup ensures the entire architecture runs locally for development and demonstration, with the same code paths that would execute in production.

---

## Running Tests

```bash
make test           # All tests with coverage
make test-unit      # Unit tests only (Spark transformations, generators, safety)
make test-dags      # DAG integrity tests
```

**Test coverage:**
- **Spark transformations**: Deduplication, sessionization, SCD Type 2, PII masking -- each tested with controlled DataFrames
- **Generators**: Event schema validation, distribution checks
- **Safety**: Prompt injection pattern matching against known attack vectors
- **DAG integrity**: Import validation, schedule verification, no cycles

---

## Snowflake Setup

```bash
# After adding credentials to .env:
make setup-snowflake
```

Runs all DDL scripts (6 schemas, 4 dimensions, 3 facts, 3 canonical tables, 2 safety tables) and seeds dimension tables with reference data (6 AI models with pricing, 3 subscription plans).

---

## What I Would Do Differently in Production

| Area | Current Approach | Production Approach | Why |
|------|-----------------|---------------------|-----|
| Stream Processing | Faust (Python) | Spark Structured Streaming or Kafka Streams | JVM-based for lower latency and better fault tolerance |
| Spark Execution | `local[*]` mode | Kubernetes pods via SparkSubmitOperator | Resource isolation, autoscaling, cost control |
| Event Buffering | In-memory only | Kafka consumer offsets + idempotent writes | At-least-once guarantees without data loss on crash |
| PII Masking | Python UDFs | Native `regexp_replace` or Pandas UDFs | 10-100x performance improvement at scale |
| Anomaly Detection | Z-score statistics | ML models (Isolation Forest, autoencoders) | Catches novel attack patterns, adapts to distribution shift |
| Injection Detection | Regex patterns | Embedding classifiers + LLM-as-judge | Catches obfuscated and novel attack techniques |
| Secret Management | Environment variables | HashiCorp Vault or AWS Secrets Manager | Rotation, access auditing, encryption at rest |
| Quality Framework | Custom rule engine | Great Expectations with Snowflake integration | Richer UI, data docs, checkpoint history |
| Monitoring | Streamlit dashboard | Grafana + Prometheus + PagerDuty | Production alerting, SLO tracking, on-call integration |
| Data Retention | SQL DELETE | Partition-based drops or Snowflake streams | More efficient for high-volume tables |
