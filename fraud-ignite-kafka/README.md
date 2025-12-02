# fraud-ignite-kafka

Stage 1 brings up local infrastructure for Apache Ignite and Kafka with a minimal schema for fraud detection experiments.

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
