I built a mini version of ChatGPT's data infrastructure from scratch — and open-sourced it.

As a data engineer, I wanted a portfolio project that goes beyond "ingest CSV into Postgres." I studied OpenAI's Data Engineer job description and reverse-engineered what their internal data platform probably looks like, then built it end-to-end.

Here's what's inside:

--- THE PIPELINE ---

Synthetic generators produce 5 event types (conversations, API usage, billing, user lifecycle, safety signals) with realistic distributions — lognormal latency, tier-gated model access, weighted session lengths.

Events flow into Kafka, get consumed by async Faust agents, and land as Hive-partitioned Parquet in MinIO (S3-compatible). Airflow orchestrates the daily batch: PySpark transforms (dedup, sessionization, SCD Type 2, PII masking) load into a Snowflake star schema. Canonical tables serve DAU, engagement, and revenue metrics as the single source of truth.

A separate hourly safety pipeline runs Z-score anomaly detection, prompt injection pattern matching (14 regex patterns across 7 attack categories), and auto-quarantines accounts that exceed violation thresholds — with human-in-the-loop review fields built in.

--- WHY THESE CHOICES ---

Every design decision maps to a real trade-off:

Window function dedup (row_number) over dropDuplicates — because in at-least-once delivery you need deterministic control over which duplicate survives.

Gap-based sessionization over fixed time windows — because splitting an active user session at an arbitrary hourly boundary produces misleading engagement numbers.

MD5 hash change detection for SCD2 — because comparing N columns per row is O(N) comparisons; hashing reduces it to one, and null handling needs explicit sentinel values to avoid NULL != NULL false positives.

Sensor mode "reschedule" instead of "poke" in Airflow — because a sleeping sensor holds a worker slot hostage for minutes while waiting for data to land.

Dimensions load before facts — because writing fact rows with missing dimension keys means broken joins and silent data loss in every downstream query.

Pre-load AND post-load quality gates — because validating inputs doesn't guarantee correct outputs when transformation logic has bugs.

--- WHAT I'D DO DIFFERENTLY IN PRODUCTION ---

Being honest about limitations matters as much as showcasing strengths:

- Python UDFs for PII masking are 10-100x slower than native Spark expressions. I chose readability for the portfolio; production needs regexp_replace chains or Pandas UDFs.
- Faust streaming has version compatibility issues with aiokafka. Production systems use Spark Structured Streaming or Kafka Streams (JVM) for stability.
- Statistical anomaly detection (Z-scores) misses novel attack patterns. Production safety systems layer in ML models and LLM-as-judge evaluations.
- In-memory event buffers lose data on crash. Production needs Kafka consumer offset management with idempotent writes.

--- THE STACK ---

Apache Airflow | Apache Kafka | PySpark | Snowflake | MinIO | Faust | Docker Compose | Streamlit

The entire thing runs locally with `docker compose up` — 7 services, 2 Airflow DAGs, 4 ETL jobs, 4 canonical builders, a safety pipeline, and a monitoring dashboard.

Code: [github.com/your-username/chatgpt-data-platform]

#DataEngineering #ApacheAirflow #ApacheSpark #Snowflake #Kafka #DataPipelines #PortfolioProject
