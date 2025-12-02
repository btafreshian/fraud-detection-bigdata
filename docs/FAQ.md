# FAQ

**Do I need the full PaySim dataset?**  
No. You can start with any CSV that matches the PaySim column names. Use `LIMIT` to ingest just a sample for demos.

**Why does training fail with a label error?**  
`make train` checks for the `isFraud` label and requires both positive and negative examples. Ensure your CSV has labeled rows before loading.

**Can I point to an existing Kafka cluster?**  
Yes. Set `KAFKA_BOOTSTRAP_SERVERS=<host>:<port>` or pass `--bootstrap` to producer/consumer CLIs. Update `docker-compose.yml` if the broker must advertise a non-localhost address.

**How do I inspect the Ignite tables?**  
`docker compose exec ignite-node /opt/ignite/apache-ignite/bin/sqlline.sh -u jdbc:ignite:thin://127.0.0.1:10800 -e "!tables"` lists them. Run `SELECT COUNT(*) FROM Transactions;` or `FraudAlerts` for counts.

**Where are alerts stored?**  
Alerts are inserted into the `FraudAlerts` Ignite table and also published to Kafka topic `fraud_alerts`.
