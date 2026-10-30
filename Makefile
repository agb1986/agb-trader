# Operator entry points. ENV_DIR is the directory holding the real env files; it defaults to
# env/ in this checkout. Compose reads db.env for interpolation, so each container receives
# only the password of its own role.
#
# BACKUP_DIR receives the nightly dumps. The backup service runs as the calling user, so the
# dumps belong to whoever ran `make up`. LAKE_DIR receives the Parquet lake, owned the same way,
# and PANEL_DIR the panel store's content-addressed bodies. MODELS_DIR holds the certified
# model artifacts the pipeline may rank with, each copied from a certification's directory as
# `<model id>/`; the pipeline and the CLI only read it. RESEARCH_DIR receives the research
# profile's trial ledger and models, on the workstation.
ENV_DIR ?= env
BACKUP_DIR ?= backups
LAKE_DIR ?= lake
PANEL_DIR ?= panel
MODELS_DIR ?= models
RESEARCH_DIR ?= research
export AGB_ENV_DIR := $(ENV_DIR)
export AGB_BACKUP_DIR := $(abspath $(BACKUP_DIR))
export AGB_BACKUP_USER := $(shell id -u):$(shell id -g)
export AGB_LAKE_DIR := $(abspath $(LAKE_DIR))
export AGB_PANEL_DIR := $(abspath $(PANEL_DIR))
export AGB_MODELS_DIR := $(abspath $(MODELS_DIR))
export AGB_PIPELINE_USER := $(shell id -u):$(shell id -g)
export AGB_RESEARCH_DIR := $(abspath $(RESEARCH_DIR))
export AGB_RESEARCH_USER := $(shell id -u):$(shell id -g)
COMPOSE := docker compose --env-file $(ENV_DIR)/db.env

.DEFAULT_GOAL := help
.PHONY: help env up down status logs agb reconcile backfill lake sync-lake research measure \
	promote sizing panel-check deploy rollback test test-fast lint

help: ## List the targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "} {printf "  %-9s %s\n", $$1, $$2}'

env: ## Copy each env/*.env.example to $(ENV_DIR)/*.env, keeping files that already exist
	@mkdir -p $(ENV_DIR)
	@for f in env/*.env.example; do \
		t=$(ENV_DIR)/$$(basename $$f .example); \
		[ -e $$t ] || { cp $$f $$t; echo "created $$t"; }; \
	done

up: ## Build the app image and bring up the stack
	@mkdir -p $(AGB_BACKUP_DIR) $(AGB_LAKE_DIR) $(AGB_PANEL_DIR) $(AGB_MODELS_DIR)
	$(COMPOSE) up --build --detach --wait

down: ## Stop the stack; the database volume is kept
	$(COMPOSE) down

status: ## Show each service's last heartbeat
	$(COMPOSE) run --rm cli status

logs: ## Follow the service logs
	$(COMPOSE) logs --follow

agb: ## Run an agb CLI command, e.g. make agb CMD=status
	$(COMPOSE) run --rm cli $(CMD)

reconcile: ## Ask the broker about a basket's orders after a crash, e.g. make reconcile REF=2026-10
	$(COMPOSE) run --rm reconcile $(REF)

backfill: ## Land the vendor's SEP, SF1 and TICKERS history from 1998 in the lake
	@mkdir -p $(AGB_LAKE_DIR)
	$(COMPOSE) run --rm pipeline python -m agb.services.pipeline backfill

lake: ## Query the lake with DuckDB, e.g. make lake SQL="SELECT count(*) FROM sep"
	$(COMPOSE) exec pipeline python -m agb.lake "$(SQL)"

sync-lake: ## Pull the server's lake into $(LAKE_DIR), read-only against the server
	@mkdir -p $(AGB_LAKE_DIR)
	uv run python -m agb.research sync --env-file $(ENV_DIR)/research.env --lake $(AGB_LAKE_DIR)

research: sync-lake ## Sync the lake, then run a research command, e.g. make research CMD="cpcv"
	@mkdir -p $(AGB_RESEARCH_DIR)
	$(COMPOSE) --profile research run --build --rm research $(CMD)

measure: ## Sample each container's memory until interrupted, e.g. make measure OUT=peaks.md
	uv run python -m agb.measure $(if $(OUT),--out $(OUT),)

promote: ## Ship a certification to the server, record it and make the model live, e.g. make promote MODEL=lgbm-2026-10
	REASON="$(REASON)" scripts/deploy/promote.sh $(MODEL) $(CMD)

sizing: ## Drive the server's sizing run, e.g. make sizing CMD=check|start|watch|stop
	scripts/deploy/sizing.sh $(CMD)

panel-check: ## Drive the panel store's corruption check, e.g. make panel-check CMD=check
	scripts/deploy/panel-check.sh $(CMD)

deploy: ## Build this commit's image, ship it to the server and restart the stack there
	scripts/deploy/deploy.sh

rollback: ## Restart the server's stack on the previously deployed image
	scripts/deploy/rollback.sh

test: ## Run the test suite against throwaway Postgres containers
	uv run pytest

test-fast: ## Run the test suite without building and starting the whole stack
	uv run pytest -m "not stack"

lint: ## Lint and check formatting
	uv run ruff check .
	uv run ruff format --check .
