.PHONY: up down demo test lint generate-data setup-snowflake clean

# ==================== Infrastructure ====================
up:
	docker compose up -d
	@echo "Waiting for services to be ready..."
	@sleep 15
	@echo "Services started. Airflow UI: http://localhost:8080 | MinIO: http://localhost:9001"

down:
	docker compose down

clean:
	docker compose down -v
	rm -rf logs/ minio_data/

# ==================== Data Generation ====================
generate-data:
	@echo "Generating synthetic event data (streaming to Kafka)..."
	python -m src.generators.runner --mode stream --duration-seconds 300 --num-users 500

generate-batch:
	@echo "Generating batch data files (writing to MinIO)..."
	python -m src.generators.runner --mode batch --num-days 30 --num-users 1000

# ==================== Snowflake ====================
setup-snowflake:
	python scripts/setup_snowflake.py

# ==================== Demo ====================
demo: up
	@echo "Waiting for Kafka to be ready..."
	@sleep 30
	@echo "Generating 5 minutes of synthetic data..."
	python -m src.generators.runner --mode stream --duration-seconds 300 --num-users 500
	@echo ""
	@echo "=== Demo Environment Ready ==="
	@echo "Airflow UI:     http://localhost:8080  (admin/admin)"
	@echo "MinIO Console:  http://localhost:9001  (minioadmin/minioadmin)"
	@echo "Kafka broker:   localhost:29092"
	@echo ""
	@echo "To launch the dashboard:  make dashboard"

dashboard:
	streamlit run src/dashboard/app.py --server.port 8501

# ==================== Testing ====================
test:
	pytest tests/ -v --cov=src --cov-report=html --cov-report=term-missing

test-unit:
	pytest tests/unit/ -v

test-dags:
	pytest tests/dags/ -v

test-integration:
	pytest tests/integration/ -v

# ==================== Code Quality ====================
lint:
	ruff check src/ dags/ tests/
	mypy src/
