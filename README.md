# fraud-ignite-kafka

Stage 1 brings up local infrastructure for Apache Ignite and Kafka with a minimal schema for fraud detection experiments.

Stage 2 adds a lightweight Python client for loading and querying the Ignite Transactions table.

Stage 3 trains a reusable sklearn pipeline from Ignite data and scores recent transactions.

## Prerequisites
- Docker
- Docker Compose

## Getting started
1. Start the stack:
   ```bash
   docker compose up -d
   ```

2. (One-time) apply the Ignite SQL schema:
   ```bash
  docker compose exec ignite-node /opt/ignite/apache-ignite/bin/sqlline.sh \
    -u jdbc:ignite:thin://127.0.0.1:10800 \
    -f /opt/ignite/init/init.sql
  ```

## Python setup

This repository targets Python 3.11+.

1. Create and activate a virtual environment:
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

### Stage 2 smoke test

Ensure the Docker Compose stack and Ignite schema from Stage 1 are running, then run:

```bash
python -m src.smoke_test
```

Example CLI entry points:

- Load a CSV file into Ignite:
  ```bash
  python -m src.load_csv_to_ignite --csv ./data/transactions.csv --limit 10000 --batch-size 1000
  ```

- Fetch a small sample from Python:
  ```bash
  python -c "from src.ignite_client import fetch_transactions; print(fetch_transactions(1))"
  ```

### Stage 3: Train & Score

Train a fraud model (requires Ignite to contain labeled transactions):

```bash
python -m src.train --max-rows 50000 --test-size 0.2 --random-state 42
```

Score recent transactions with the saved pipeline:

```bash
python -m src.score --limit 10
```

## Verification

### Kafka
- List existing topics (should succeed even if none exist):
  ```bash
  docker compose exec kafka /opt/bitnami/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --list
  ```
- Create a test topic to confirm read/write paths:
  ```bash
  docker compose exec kafka /opt/bitnami/kafka/bin/kafka-topics.sh \
    --create --topic test-fraud --partitions 1 --replication-factor 1 \
    --bootstrap-server localhost:9092
  ```

### Apache Ignite
- Open the SQL shell to inspect objects:
  ```bash
  docker compose exec -it ignite-node /opt/ignite/apache-ignite/bin/sqlline.sh \
    -u jdbc:ignite:thin://127.0.0.1:10800
  ```
  From the shell, list the tables:
  ```sql
  !tables
  ```

- Run a quick query to confirm the schema is present:
  ```bash
  docker compose exec ignite-node /opt/ignite/apache-ignite/bin/sqlline.sh \
    -u jdbc:ignite:thin://127.0.0.1:10800 \
    -e "SELECT table_name FROM information_schema.tables WHERE table_schema = 'PUBLIC';"
  ```

## Troubleshooting
- Port conflicts:
  - Ignite thin client binds to `127.0.0.1:10800`.
  - Kafka binds to `0.0.0.0:9092` with advertised listener `localhost:9092`.
  - Zookeeper uses `2181`.
  Stop any process occupying these ports before starting the stack.

- Reset the stack (removes containers and volumes):
  ```bash
  docker compose down -v
  ```
