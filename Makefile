# Local-first developer targets. Requires Python 3.11+ and optionally uv.
# Prefer `uv` when available; fall back to python -m pip / venv.

PYTHON ?= python3
UV := $(shell command -v uv 2>/dev/null)

.PHONY: help install install-dev lint format typecheck test test-unit test-integration pre-commit-install pre-commit clean compose-up compose-down generate-ticks create-topics ingest bronze silver gold replay-dlq ci

help:
	@echo "realtime-market-stream targets:"
	@echo "  make install             Install the package (editable)"
	@echo "  make install-dev         Install package + dev extras (ruff, mypy, pytest, pre-commit)"
	@echo "  make lint                Run ruff check"
	@echo "  make format              Run ruff format"
	@echo "  make typecheck           Run mypy"
	@echo "  make test                Run pytest (unit + in-process integration)"
	@echo "  make test-unit           Run tests/unit only"
	@echo "  make test-integration    Run tests/integration (no live broker)"
	@echo "  make ci                  Run the same checks CI runs (lint + format check + mypy + test)"
	@echo "  make generate-ticks      Print synthetic ticks as JSONL (COUNT=10 SYMBOLS=AAPL,MSFT)"
	@echo "  make ingest              Publish ticks to Redpanda (COUNT=20 SOURCE=synthetic)"
	@echo "  make bronze              Land raw-ticks into Bronze (MAX_RECORDS=20 BATCH_SIZE=10)"
	@echo "  make silver              Dedup/enrich/window into Silver (MAX_RECORDS=20 WINDOW_SECONDS=60)"
	@echo "  make gold                Score Silver bars (MAX_RECORDS=20 LOOKBACK=20 Z_THRESHOLD=3.0)"
	@echo "  make replay-dlq          Replay DLQ onto raw-ticks (MAX_RECORDS=20 DRY_RUN=1 INSPECT=1)"
	@echo "  make create-topics       Idempotently create Redpanda topics (DRY_RUN=1 to preview)"
	@echo "  make pre-commit-install  Install git hooks (pre-commit + pre-push)"
	@echo "  make pre-commit          Run all pre-commit hooks on all files"
	@echo "  make compose-up          Start local infra (Redpanda, MinIO, ...)"
	@echo "  make compose-down        Stop local infra"
	@echo "  make clean               Remove caches and build artifacts"

install:
ifdef UV
	uv pip install -e .
else
	$(PYTHON) -m pip install -e .
endif

install-dev:
ifdef UV
	uv pip install -e ".[dev]"
else
	$(PYTHON) -m pip install -e ".[dev]"
endif

lint:
	ruff check src tests apps scripts

format:
	ruff format src tests apps scripts

typecheck:
	mypy --config-file pyproject.toml

test:
	pytest

test-unit:
	pytest tests/unit

test-integration:
	pytest tests/integration -m "integration and not broker"

# Mirror GitHub Actions CI jobs locally before pushing.
ci: lint
	ruff format --check src tests apps scripts
	$(MAKE) typecheck
	$(MAKE) test

COUNT ?= 10
SYMBOLS ?=
RATE ?=
DRY_RUN ?=
SOURCE ?= synthetic
WS_URL ?=
MAX_RECORDS ?=
BATCH_SIZE ?= 50
WINDOW_SECONDS ?= 60
LOOKBACK ?= 20
Z_THRESHOLD ?= 3.0
DATA_ROOT ?=
INSPECT ?=
ERROR_CONTAINS ?=

generate-ticks:
	$(PYTHON) scripts/generate_ticks.py --count $(COUNT) $(if $(SYMBOLS),--symbols $(SYMBOLS),) $(if $(RATE),--rate $(RATE),)

ingest:
	$(PYTHON) scripts/run_ingestion.py --source $(SOURCE) --count $(COUNT) $(if $(WS_URL),--ws-url $(WS_URL),)

bronze:
	$(PYTHON) scripts/run_bronze.py --batch-size $(BATCH_SIZE) $(if $(MAX_RECORDS),--max-records $(MAX_RECORDS),) $(if $(DATA_ROOT),--data-root $(DATA_ROOT),)

silver:
	$(PYTHON) scripts/run_silver.py --window-seconds $(WINDOW_SECONDS) $(if $(MAX_RECORDS),--max-records $(MAX_RECORDS),) $(if $(DATA_ROOT),--data-root $(DATA_ROOT),)

gold:
	$(PYTHON) scripts/run_gold.py --lookback $(LOOKBACK) --z-threshold $(Z_THRESHOLD) $(if $(MAX_RECORDS),--max-records $(MAX_RECORDS),) $(if $(DATA_ROOT),--data-root $(DATA_ROOT),)

replay-dlq:
	$(PYTHON) scripts/run_dlq.py $(if $(MAX_RECORDS),--max-records $(MAX_RECORDS),) $(if $(DRY_RUN),--dry-run,) $(if $(INSPECT),--inspect,) $(if $(ERROR_CONTAINS),--error-contains $(ERROR_CONTAINS),)

create-topics:
	$(PYTHON) scripts/create_topics.py $(if $(DRY_RUN),--dry-run,)

pre-commit-install:
	pre-commit install --hook-type pre-commit --hook-type pre-push

pre-commit:
	pre-commit run --all-files

compose-up:
	docker compose up -d

compose-down:
	docker compose down

clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type d -name .mypy_cache -exec rm -rf {} +
	find . -type d -name .ruff_cache -exec rm -rf {} +
	find . -type d -name .pytest_cache -exec rm -rf {} +
	rm -rf build dist *.egg-info .coverage htmlcov
