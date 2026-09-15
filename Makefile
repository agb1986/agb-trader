# Operator entry points. ENV_DIR is the directory holding the real env files; it defaults to
# env/ in this checkout. Compose reads db.env for interpolation, so each container receives
# only the password of its own role.
ENV_DIR ?= env
export AGB_ENV_DIR := $(ENV_DIR)
COMPOSE := docker compose --env-file $(ENV_DIR)/db.env

.DEFAULT_GOAL := help
.PHONY: help env up down status logs agb test test-fast lint

help: ## List the targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "} {printf "  %-7s %s\n", $$1, $$2}'

env: ## Copy each env/*.env.example to $(ENV_DIR)/*.env, keeping files that already exist
	@mkdir -p $(ENV_DIR)
	@for f in env/*.env.example; do \
		t=$(ENV_DIR)/$$(basename $$f .example); \
		[ -e $$t ] || { cp $$f $$t; echo "created $$t"; }; \
	done

up: ## Build the app image and bring up the stack
	$(COMPOSE) up --build --detach --wait

down: ## Stop the stack; the database volume is kept
	$(COMPOSE) down

status: ## Show each service's last heartbeat
	$(COMPOSE) run --rm cli status

logs: ## Follow the service logs
	$(COMPOSE) logs --follow

agb: ## Run an agb CLI command, e.g. make agb CMD=status
	$(COMPOSE) run --rm cli $(CMD)

test: ## Run the test suite against throwaway Postgres containers
	uv run pytest

test-fast: ## Run the test suite without building and starting the whole stack
	uv run pytest -m "not stack"

lint: ## Lint and check formatting
	uv run ruff check .
	uv run ruff format --check .
