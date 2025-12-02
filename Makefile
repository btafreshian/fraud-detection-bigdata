VENV = .venv
PYTHON = $(VENV)/bin/python
PIP = $(VENV)/bin/pip
DOCKER_COMPOSE = docker compose

CSV ?= ./data/transactions.csv
LIMIT ?= 200
THRESH ?= 0.7
MODE ?= synthetic
SLEEP_MS ?= 25

.PHONY: up down reset venv install smoke load train consume produce hybrid test

up:
	$(DOCKER_COMPOSE) up -d

down:
	$(DOCKER_COMPOSE) down

reset:
	$(DOCKER_COMPOSE) down -v

venv:
	@test -d $(VENV) || python -m venv $(VENV)

install: venv
	$(PIP) install -r requirements.txt

smoke: install
	$(PYTHON) -m src.smoke_test

load: install
	@if [ ! -f "$(CSV)" ]; then \
		echo "[fraud-demo] ERROR: CSV file '$(CSV)' not found. Set CSV=<path> to a local PaySim file."; \
		exit 1; \
	fi
	$(PYTHON) -m src.load_csv_to_ignite --csv $(CSV) --limit $(LIMIT)

train: install
	$(PYTHON) -m src.train

consume: install
	$(PYTHON) -m src.consume --threshold $(THRESH)

produce: install
	@if [ "$(MODE)" = "csv" ]; then \
		if [ -z "$(CSV)" ]; then echo "[fraud-demo] ERROR: Set CSV=<path> when MODE=csv"; exit 1; fi; \
		$(PYTHON) -m src.produce --csv $(CSV) --limit $(LIMIT) --sleep-ms $(SLEEP_MS); \
	else \
		$(PYTHON) -m src.produce --synthetic --limit $(LIMIT) --sleep-ms $(SLEEP_MS); \
	fi

hybrid: install
	$(PYTHON) -m src.hybrid --limit $(LIMIT)

test: install
	$(PYTHON) -m pytest
