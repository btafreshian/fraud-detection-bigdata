# fraud-ignite-kafka

Stages 1–5 set up Apache Ignite and Kafka, then train and deploy a lightweight
fraud detection demo. Stage 6 adds quality-of-life tooling so the entire flow
can be run end-to-end with a handful of commands.

## Demo in 5 commands

```bash
make up
make venv && make install
make load CSV=./data/transactions.csv LIMIT=100000   # optional when you have PaySim
make train                                            # requires labeled rows in Ignite
make consume THRESH=0.7                               # terminal A
make produce MODE=synthetic LIMIT=200 SLEEP_MS=25     # terminal B
```

After a few messages, run the hybrid report:

```bash
make hybrid LIMIT=20
```

Prefer a helper? `./scripts/demo.sh` echoes the steps above and sends a short
synthetic burst (assumes the consumer is already running).

## Make targets

- `make up` / `make down` / `make reset` — manage the Docker Compose stack.
- `make venv` / `make install` — create `.venv` and install Python deps.
- `make smoke` — run a minimal Ignite CRUD test.
- `make load CSV=... LIMIT=...` — ingest a PaySim-style CSV into Ignite.
- `make train` — train and save `artifacts/model.joblib` once labeled data is loaded.
- `make consume THRESH=0.7` — score the Kafka stream and emit alerts.
- `make produce MODE=synthetic LIMIT=200 SLEEP_MS=25` — send transactions from synthetic data (or `MODE=csv CSV=...`).
- `make hybrid LIMIT=20` — print the rule+ML risk report for suspicious rows.
- `make test` — run the lightweight pytest suite.

## Environment & troubleshooting notes

- Ports: Ignite thin client `127.0.0.1:10800`; Kafka `9092`; Zookeeper `2181`.
- Schema check: `docker compose exec ignite-node /opt/ignite/apache-ignite/bin/sqlline.sh -u jdbc:ignite:thin://127.0.0.1:10800 -f /opt/ignite/init/init.sql` (idempotent) and `!tables` inside the shell.
- Verify alerts landed: `docker compose exec ignite-node /opt/ignite/apache-ignite/bin/sqlline.sh -u jdbc:ignite:thin://127.0.0.1:10800 -e "SELECT COUNT(*) FROM FraudAlerts;"`.
- Kafka sanity: `docker compose exec kafka /opt/bitnami/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list`.
- If any CLI reports Ignite/Kafka unreachable, ensure `make up` succeeded and no other processes occupy the ports.

## Python & training tips

- The project targets Python 3.11+. All entry points are runnable via
  `python -m src.<module>` after `make install`.
- Training requires labeled rows (`isFraud`) in Ignite. Load a CSV with labels
  before calling `make train`; the CLI will abort if no labeled data is found.
- Consumers, scorers, and the hybrid report exit early with clear messages when
  the model artifact or infrastructure is missing.

## What files get generated

- `artifacts/model.joblib` — saved sklearn pipeline after training.
- `scripts/demo.sh` — helper to replay a short synthetic streaming run.
- User-provided data such as `./data/transactions.csv` (not committed).
- Docker volumes (`docker compose down -v` to reset) hold Ignite/Kafka state.

## Hybrid report refresher

`make hybrid LIMIT=20` fetches suspicious transactions (high amount or
zero-balance transfers/cash-outs), scores them with the trained pipeline, boosts
rule hits to a minimum `final_risk` of 0.95, and prints a ranked table. Re-run
after new training or data loads to see updated ordering.
