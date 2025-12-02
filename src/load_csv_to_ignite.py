"""Load a PaySim-like CSV file into Apache Ignite."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Iterable, List
from uuid import uuid4

from .config import DEFAULT_BATCH_SIZE
from .ignite_client import insert_transactions
from .schema import from_dict, to_row_dict


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load transactions from a PaySim-like CSV into Apache Ignite."
    )
    parser.add_argument(
        "--csv",
        required=True,
        help="Path to the CSV file containing transactions.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional limit on number of rows to load (default: entire file).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Number of rows to insert per batch (default: {DEFAULT_BATCH_SIZE}).",
    )
    return parser.parse_args()


def read_csv_rows(path: Path, limit: int | None) -> Iterable[dict]:
    with path.open(newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        for idx, row in enumerate(reader):
            if limit is not None and idx >= limit:
                break
            if not row.get("tx_id"):
                row["tx_id"] = str(uuid4())
            yield row


def buffer_batches(rows: Iterable[dict], batch_size: int) -> Iterable[List[dict]]:
    buffer: List[dict] = []
    for row in rows:
        buffer.append(row)
        if len(buffer) >= batch_size:
            yield buffer
            buffer = []
    if buffer:
        yield buffer


def load_csv(csv_path: Path, limit: int | None, batch_size: int) -> int:
    batches = buffer_batches(read_csv_rows(csv_path, limit), batch_size)
    total_inserted = 0

    for batch_num, batch in enumerate(batches, start=1):
        transactions = [from_dict(row) for row in batch]
        inserted_ids = insert_transactions(to_row_dict(tx) for tx in transactions)
        total_inserted += len(inserted_ids)
        print(f"Inserted batch {batch_num}: {len(inserted_ids)} rows (total {total_inserted})")

    return total_inserted


def main() -> None:
    args = parse_args()
    csv_path = Path(args.csv)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    total = load_csv(csv_path=csv_path, limit=args.limit, batch_size=args.batch_size)
    print(f"Finished loading {total} rows from {csv_path}")


if __name__ == "__main__":
    main()

