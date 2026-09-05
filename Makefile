# Local-first developer targets. Requires Python 3.11+ and optionally uv.
# Prefer `uv` when available; fall back to python -m pip / venv.

PYTHON ?= python3
UV := $(shell command -v uv 2>/dev/null)

.PHONY: help install install-dev lint format typecheck test pre-commit-install pre-commit clean compose-up compose-down generate-ticks create-topics ci

help:
	@echo "realtime-market-stream targets:"
	@echo "  make install             Install the package (editable)"
	@echo "  make install-dev         Install package + dev extras (ruff, mypy, pytest, pre-commit)"
	@echo "  make lint                Run ruff check"
	@echo "  make format              Run ruff format"
	@echo "  make typecheck           Run mypy"
	@echo "  make test                Run pytest"
	@echo "  make ci                  Run the same checks CI runs (lint + format check + mypy + test)"
	@echo "  make generate-ticks      Print synthetic ticks as JSONL (COUNT=10 SYMBOLS=AAPL,MSFT)"
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

# Mirror GitHub Actions CI jobs locally before pushing.
ci: lint
	ruff format --check src tests apps scripts
	$(MAKE) typecheck
	$(MAKE) test

COUNT ?= 10
SYMBOLS ?=
RATE ?=
DRY_RUN ?=

generate-ticks:
	$(PYTHON) scripts/generate_ticks.py --count $(COUNT) $(if $(SYMBOLS),--symbols $(SYMBOLS),) $(if $(RATE),--rate $(RATE),)

create-topics:
	$(PYTHON) scripts/create_topics.py $(if $(DRY_RUN),--dry-run,)

# Install both commit and push hooks so CI-equivalent checks run before push.
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
