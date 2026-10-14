#!/usr/bin/env python3
"""Order-independent reconciliation of two R2 JSONL inventories."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any, Iterator


def canonical(row: dict[str, Any]) -> str:
    return json.dumps(row, sort_keys=True, separators=(",", ":"))


def digest(row: dict[str, Any]) -> str:
    return hashlib.sha256(canonical(row).encode("utf-8")).hexdigest()


def rows(path: Path) -> Iterator[tuple[str, str, dict[str, Any]]]:
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                row = json.loads(line)
                yield str(row["bucket"]), str(row["key"]), row


def sample(values: list[dict[str, Any]], value: dict[str, Any]) -> None:
    if len(values) < 20:
        values.append(value)


def ingest(con: sqlite3.Connection, table: str, path: Path) -> dict[str, int]:
    totals = {"rows": 0, "unique": 0, "duplicate_identical": 0, "duplicate_conflicting": 0}
    statement = f"INSERT INTO {table}(bucket,key,digest,size,raw) VALUES(?,?,?,?,?)"
    for bucket, key, row in rows(path):
        totals["rows"] += 1
        encoded = canonical(row)
        try:
            con.execute(statement, (bucket, key, digest(row), int(row["size"]), encoded))
            totals["unique"] += 1
        except sqlite3.IntegrityError:
            prior = con.execute(f"SELECT digest FROM {table} WHERE bucket=? AND key=?", (bucket, key)).fetchone()
            if prior and prior[0] == digest(row):
                totals["duplicate_identical"] += 1
            else:
                totals["duplicate_conflicting"] += 1
        if totals["rows"] % 100_000 == 0:
            con.commit()
    con.commit()
    return totals


def sql_count(con: sqlite3.Connection, query: str) -> int:
    return int(con.execute(query).fetchone()[0])


def sql_bytes(con: sqlite3.Connection, query: str) -> int:
    return int(con.execute(query).fetchone()[0] or 0)


def compare(old_path: Path, new_path: Path, database: Path) -> dict[str, Any]:
    database.unlink(missing_ok=True)
    con = sqlite3.connect(database)
    con.executescript("""
        PRAGMA journal_mode=OFF;
        PRAGMA synchronous=OFF;
        CREATE TABLE old_rows (
          bucket TEXT NOT NULL, key TEXT NOT NULL, digest TEXT NOT NULL,
          size INTEGER NOT NULL, raw TEXT NOT NULL,
          PRIMARY KEY(bucket,key)
        ) WITHOUT ROWID;
        CREATE TABLE new_rows (
          bucket TEXT NOT NULL, key TEXT NOT NULL, digest TEXT NOT NULL,
          size INTEGER NOT NULL, raw TEXT NOT NULL,
          PRIMARY KEY(bucket,key)
        ) WITHOUT ROWID;
    """)
    inventory_rows = {"old": ingest(con, "old_rows", old_path), "new": ingest(con, "new_rows", new_path)}
    joins = "old_rows o JOIN new_rows n ON o.bucket=n.bucket AND o.key=n.key"
    counts = {
        "identical": sql_count(con, f"SELECT COUNT(*) FROM {joins} WHERE o.digest=n.digest"),
        "changed": sql_count(con, f"SELECT COUNT(*) FROM {joins} WHERE o.digest<>n.digest"),
        "old_only": sql_count(con, "SELECT COUNT(*) FROM old_rows o LEFT JOIN new_rows n ON o.bucket=n.bucket AND o.key=n.key WHERE n.key IS NULL"),
        "new_only": sql_count(con, "SELECT COUNT(*) FROM new_rows n LEFT JOIN old_rows o ON o.bucket=n.bucket AND o.key=n.key WHERE o.key IS NULL"),
    }
    sizes = {
        "old_only": sql_bytes(con, "SELECT SUM(o.size) FROM old_rows o LEFT JOIN new_rows n ON o.bucket=n.bucket AND o.key=n.key WHERE n.key IS NULL"),
        "new_only": sql_bytes(con, "SELECT SUM(n.size) FROM new_rows n LEFT JOIN old_rows o ON o.bucket=n.bucket AND o.key=n.key WHERE o.key IS NULL"),
        "changed_old": sql_bytes(con, f"SELECT SUM(o.size) FROM {joins} WHERE o.digest<>n.digest"),
        "changed_new": sql_bytes(con, f"SELECT SUM(n.size) FROM {joins} WHERE o.digest<>n.digest"),
    }
    samples: dict[str, list[dict[str, Any]]] = {"old_only": [], "new_only": [], "changed": []}
    for (raw,) in con.execute("SELECT o.raw FROM old_rows o LEFT JOIN new_rows n ON o.bucket=n.bucket AND o.key=n.key WHERE n.key IS NULL LIMIT 20"):
        sample(samples["old_only"], json.loads(raw))
    for (raw,) in con.execute("SELECT n.raw FROM new_rows n LEFT JOIN old_rows o ON o.bucket=n.bucket AND o.key=n.key WHERE o.key IS NULL LIMIT 20"):
        sample(samples["new_only"], json.loads(raw))
    for old_raw, new_raw in con.execute(f"SELECT o.raw,n.raw FROM {joins} WHERE o.digest<>n.digest LIMIT 20"):
        sample(samples["changed"], {"old": json.loads(old_raw), "new": json.loads(new_raw)})
    con.close()
    return {"comparison": "order-independent bucket/key reconciliation", "old_inventory": str(old_path), "new_inventory": str(new_path), "inventory_rows": inventory_rows, "counts": counts, "bytes": sizes, "samples": samples}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("old", type=Path)
    parser.add_argument("new", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--work-db", type=Path, required=True)
    args = parser.parse_args()
    result = compare(args.old, args.new, args.work_db)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result["counts"], sort_keys=True))


if __name__ == "__main__":
    main()
