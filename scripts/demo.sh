#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

cat <<'STEPS'
=== Fraud demo quick start ===
1) Start infrastructure:
   make up
2) Prepare Python tooling:
   make venv && make install
3) Load data (if you have a CSV):
   make load CSV=./data/transactions.csv LIMIT=100000
4) Train the model (once you have labeled data in Ignite):
   make train
5) In another terminal, start the consumer:
   make consume THRESH=0.7

This script will now send 200 synthetic messages and run the hybrid report.
STEPS

if [ ! -x .venv/bin/python ]; then
  echo "[fraud-demo] Creating virtual environment and installing dependencies..."
  make venv >/dev/null
  make install
fi

echo "[fraud-demo] Publishing synthetic transactions..."
make produce MODE=synthetic LIMIT=200 SLEEP_MS=25

echo "[fraud-demo] Running hybrid report (requires a trained model and data in Ignite)..."
make hybrid LIMIT=20
