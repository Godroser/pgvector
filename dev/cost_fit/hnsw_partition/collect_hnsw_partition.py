#!/usr/bin/env python3
"""
Collect HNSW Index Scan timings on partition-pruned ANN queries (part_vec_p / partsupp_vec_p).
Prereq: hnsw_partition/00 + 01 SQL applied; HNSW exists only on idx 16,49,82 children.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import statistics
import sys

_COST_FIT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _COST_FIT_ROOT not in sys.path:
    sys.path.insert(0, _COST_FIT_ROOT)

from fit_common import (  # noqa: E402
    exclusive_total_time,
    explain_analyze_json,
    load_table_stats,
    psql_sql,
    walk_plans,
)
from workloads import hnsw_partition_scan_workloads  # noqa: E402

FORCE_INDEX = "SET LOCAL enable_seqscan TO off;"


def _session_prelude() -> str:
    path = os.path.join(_COST_FIT_ROOT, "session_prelude.sql")
    with open(path, encoding="utf-8") as f:
        return f.read().strip() + "\n"


def _tx_prefix(knobs: dict) -> str:
    lines = [_session_prelude(), FORCE_INDEX]
    for k in sorted(knobs.keys()):
        lines.append(f"SET LOCAL {k} TO {knobs[k]};")
    return "\n".join(lines) + "\n"


def _parse_limit(sql: str) -> int:
    m = re.search(r"\bLIMIT\s+(\d+)\s*;", sql, re.I | re.DOTALL)
    return int(m.group(1)) if m else 0


def _pick_hnsw_scan(root: dict) -> dict | None:
    for node in walk_plans(root["Plan"]):
        if node.get("Node Type") != "Index Scan":
            continue
        iname = (node.get("Index Name") or "").lower()
        rel = (node.get("Relation Name") or "").lower()
        if "hnsw" in iname and (
            rel.startswith("part_vec_p_p") or rel.startswith("partsupp_vec_p_p")
        ):
            return node
    return None


def _load_two_column_ranges(sql: str) -> list[tuple[int, int]]:
    out = psql_sql(sql.strip(), tuples_only=True)
    rows: list[tuple[int, int]] = []
    for line in out.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split("|")
        if len(parts) != 2:
            continue
        rows.append((int(parts[0]), int(parts[1])))
    return rows


def load_bounds_from_db() -> tuple[list[tuple[int, int]], list[tuple[int, int]]]:
    part_sql = """
SELECT p_lo, p_hi FROM _cost_fit_hnsw_part_bounds WHERE idx IN (16, 49, 82) ORDER BY idx;
"""
    ps_sql = """
SELECT ps_lo, ps_hi FROM _cost_fit_hnsw_ps_bounds WHERE idx IN (16, 49, 82) ORDER BY idx;
"""
    pr = _load_two_column_ranges(part_sql)
    sr = _load_two_column_ranges(ps_sql)
    if len(pr) != 3 or len(sr) != 3:
        raise SystemExit(
            "Expected three partition bounds rows; run hnsw_partition/00_create_partitioned_tables.sql "
            f"and 01. got part={len(pr)} partsupp={len(sr)}"
        )
    return pr, sr


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--out",
        default=os.path.join(_COST_FIT_ROOT, "data", "hnsw_partition_scan_samples.jsonl"),
    )
    ap.add_argument("--repeats", type=int, default=1)
    ap.add_argument("--target", type=int, default=200)
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()
    os.makedirs(os.path.dirname(args.out), exist_ok=True)

    part_ranges, ps_ranges = load_bounds_from_db()
    stats = load_table_stats()
    workloads = hnsw_partition_scan_workloads(part_ranges, ps_ranges, args.target)
    if args.limit:
        workloads = workloads[: args.limit]

    n_ok = 0
    with open(args.out, "w", encoding="utf-8") as fout:
        for tag, sql, knobs in workloads:
            runs: list = []
            for _ in range(args.repeats):
                try:
                    root = explain_analyze_json(sql, tx_prefix=_tx_prefix(knobs))
                except Exception as e:
                    print(f"[fail] {tag}: {e}", file=sys.stderr)
                    continue
                runs.append(_pick_hnsw_scan(root))
            if not runs or all(n is None for n in runs):
                print(f"[warn] {tag}: no HNSW Index Scan on partition child", file=sys.stderr)
                continue
            cand = next(n for n in runs if n is not None)
            rel = (cand.get("Relation Name") or "").lower()
            if rel not in stats:
                print(f"[warn] {tag}: relation {rel!r} missing from pg_class stats", file=sys.stderr)
                continue
            tuples, pages = stats[rel]
            vals = [exclusive_total_time(n) for n in runs if n is not None]
            if not vals:
                continue
            ex = float(statistics.median(vals))
            row = {
                "tag": tag,
                "sql": sql,
                "relation": rel,
                "index_name": cand.get("Index Name"),
                "node_type": "Index Scan (hnsw, partition)",
                "relpages": pages,
                "reltuples": tuples,
                "exclusive_ms": ex,
                "plan_rows": float(cand.get("Plan Rows") or 0),
                "actual_rows": float(cand.get("Actual Rows") or 0),
                "plan_width": int(cand.get("Plan Width") or 0),
                "limit_k": _parse_limit(sql),
                "ef_search": int(knobs["hnsw.ef_search"]),
            }
            fout.write(json.dumps(row, ensure_ascii=False) + "\n")
            n_ok += 1
            print(tag, ex, "ms")

    print("wrote", args.out, "count", n_ok)
    if n_ok < 200:
        print(f"[warn] only {n_ok} samples (<200).", file=sys.stderr)


if __name__ == "__main__":
    main()
