.PHONY: help install install-dev test lint format clean docker-build docker-run pipeline

# Default target
.DEFAULT_GOAL := help

# Variables
PYTHON := python3
PIP := pip3
DOCKER_IMAGE := churn-classifier
DOCKER_TAG := latest

help:  ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $1, $2}'

install:  ## Install production dependencies
	$(PIP) install -r requirements.txt

install-dev:  ## Install development dependencies
	$(PIP) install -r requirements-dev.txt
	chmod +x scripts/*.sh

setup:  ## Setup project directories and DVC
	$(PYTHON) -c "from src.utils import setup_directories; setup_directories()"
	./scripts/setup_dvc.sh

data:  ## Generate sample data
	$(PYTHON) -c "from src.utils import generate_sample_data, setup_directories; setup_directories(); generate_sample_data('data/raw/churn_data.csv', n_samples=1000)"

preprocess:  ## Run preprocessing
	$(PYTHON) -m src.preprocess

train:  ## Train model
	$(PYTHON) -m src.train

evaluate:  ## Evaluate model
	$(PYTHON) -m src.evaluate

predict:  ## Make predictions (requires INPUT=file.csv)
	$(PYTHON) -m src.predict --input $(INPUT) --output predictions.csv

pipeline:  ## Run complete pipeline
	./scripts/run_pipeline.sh

test:  ## Run unit tests
	pytest tests/ -v --cov=src --cov-report=term-missing

test-coverage:  ## Run tests with HTML coverage report
	pytest tests/ -v --cov=src --cov-report=html
	@echo "Coverage report: htmlcov/index.html"

lint:  ## Run linting checks
	ruff check src/ tests/
	black --check src/ tests/
	flake8 src/ tests/ --max-line-length=100

format:  ## Format code with black
	black src/ tests/
	ruff check src/ tests/ --fix

docker-build:  ## Build Docker image
	./scripts/docker_build.sh

docker-build-dev:  ## Build development Docker image
	./scripts/docker_build.sh --dev

docker-run:  ## Run Docker container
	docker run --rm \
		-v $(PWD)/data:/app/data \
		-v $(PWD)/models:/app/models \
		-v $(PWD)/params.yaml:/app/params.yaml \
		$(DOCKER_IMAGE):$(DOCKER_TAG)

docker-compose-up:  ## Start all services with docker-compose
	docker-compose -f docker/docker-compose.yml up

docker-compose-down:  ## Stop all services
	docker-compose -f docker/docker-compose.yml down

dvc-init:  ## Initialize DVC
	./scripts/setup_dvc.sh

dvc-pull:  ## Pull data from DVC remote
	dvc pull

dvc-push:  ## Push data to DVC remote
	dvc push

dvc-repro:  ## Reproduce DVC pipeline
	dvc repro

clean:  ## Clean generated files
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".coverage" -delete
	find . -type d -name "htmlcov" -exec rm -rf {} +
	rm -f metrics/metrics.json
	@echo "✅ Cleaned!"

clean-data:  ## Clean data files (WARNING: removes generated data)
	rm -rf data/raw/*.csv data/processed/*.csv
	@echo "Data files removed!"

clean-models:  ## Clean model files
	rm -rf models/*.pkl
	@echo "⚠️  Model files removed!"

clean-all: clean clean-data clean-models  ## Clean everything
	@echo "🧹 Everything cleaned!"

api-run:  ## Run FastAPI server
	uvicorn src.api.main:app --host 0.0.0.0 --port 8000 --reload

api-run-prod:  ## Run FastAPI in production mode
	uvicorn src.api.main:app --host 0.0.0.0 --port 8000 --workers 4

api-test:  ## Test API endpoints
	pytest tests/test_api.py -v

api-docker:  ## Run API in Docker
	docker-compose -f docker/docker-compose.yml up api

api-docker-postgres:  ## Run API with PostgreSQL
	docker-compose -f docker/docker-compose.yml up postgres api-postgres

db-init:  ## Initialize database
	python -c "from src.api.database import Base, engine; Base.metadata.create_all(bind=engine); print('Database initialized')"

db-migrate:  ## Run database migrations (if using Alembic)
	alembic upgrade head

db-reset:  ## Reset database
	rm -f churn_predictions.db
	$(MAKE) db-init