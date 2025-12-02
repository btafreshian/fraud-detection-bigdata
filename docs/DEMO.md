# Demo script

A compact script to replay the end-to-end flow without reading source files.

```bash
# 1) Start infrastructure
make up

# 2) Install Python deps
make venv && make install

# 3) (Optional) Load PaySim sample
make load CSV=./data/transactions.csv LIMIT=50000

# 4) Train the baseline model
make train

# 5) Open two terminals for streaming
# Terminal A
make consume THRESH=0.7
# Terminal B (synthetic stream)
make produce MODE=synthetic LIMIT=200 SLEEP_MS=25

# 6) Hybrid report (new terminal)
make hybrid LIMIT=20

# 7) Reset when done
make reset
```

Expected outputs:
- `make train` prints confusion matrix + precision/recall/F1/PR-AUC and writes `artifacts/model.joblib`.
- Consumer logs `[ALERT] tx_id=... prob=... rule_hit=...` as alerts trigger.
- Hybrid report renders a markdown table of suspicious transactions sorted by `final_risk`.
