# fraud-ignite-kafka

Stage 1 brings up local infrastructure for Apache Ignite and Kafka with a minimal schema for fraud detection experiments.

## Prerequisites
- Docker
- Docker Compose
- Python 3.11+

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

## Python setup (Stage 2)
1. Create and activate a virtual environment:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
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

### Stage 2 smoke tests
- Insert and fetch a single row:
  ```bash
  python -m src.smoke_test
  ```
- Load a CSV file in batches (example):
  ```bash
  python -m src.load_csv_to_ignite --csv ./data/transactions.csv --limit 10000 --batch-size 1000
  ```
- Import check for the Ignite client:
  ```bash
  python -c "from src.ignite_client import fetch_transactions; print(fetch_transactions(1))"
  ```

Both the loader and smoke test accept optional Ignite connection overrides:
```bash
python -m src.smoke_test --host 127.0.0.1 --port 10800
python -m src.load_csv_to_ignite --csv ./data/transactions.csv --host 127.0.0.1 --port 10800
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
