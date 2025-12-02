"""CLI to load PaySim-like CSV data into Apache Ignite."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Dict, Iterable
from uuid import uuid4

from src import config
from src.ignite_client import insert_transactions
from src.schema import Transaction, to_row_dict


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load a PaySim-like CSV into Ignite Transactions table")
    parser.add_argument("--csv", required=True, help="Path to CSV file")
    parser.add_argument("--limit", type=int, default=None, help="Optional limit on rows to load")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=config.DEFAULT_BATCH_SIZE,
        help=f"Number of rows to insert per batch (default: {config.DEFAULT_BATCH_SIZE})",
    )
    return parser.parse_args()


def _normalize_row(row: Dict[str, str]) -> Dict[str, object]:
    """Fill defaults and generate tx_id when absent."""
    normalized = dict(row)
    if not normalized.get("tx_id"):
        normalized["tx_id"] = str(uuid4())

    # Ensure all expected keys exist even if missing in the CSV
    tx = Transaction.from_dict(normalized)
    return to_row_dict(tx)


def _read_csv_rows(path: Path, limit: int | None) -> Iterable[Dict[str, object]]:
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        count = 0
        for row in reader:
            yield _normalize_row(row)
            count += 1
            if limit is not None and count >= limit:
                break


def main() -> None:
    args = parse_args()
    csv_path = Path(args.csv)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    batch_size = max(1, int(args.batch_size))
    buffer: list[Dict[str, object]] = []
    total_inserted = 0

    def flush_batch(batch: list[Dict[str, object]]) -> None:
        nonlocal total_inserted
        if not batch:
            return
        insert_transactions(batch)
        total_inserted += len(batch)
        print(f"Inserted {total_inserted} rows so far...")

    for row in _read_csv_rows(csv_path, args.limit):
        buffer.append(row)
        if len(buffer) >= batch_size:
            flush_batch(buffer)
            buffer = []

    if buffer:
        flush_batch(buffer)

    print(f"Completed loading. Total rows inserted: {total_inserted}")


if __name__ == "__main__":
    main()
