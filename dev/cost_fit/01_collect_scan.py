#!/usr/bin/env python3
"""
Collect Seq Scan samples. Default: >=55 workloads (workloads.scan_workloads).
Output: data/scan_samples.jsonl
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys

from fit_common import (
    exclusive_total_time,
    explain_analyze_json,
    load_table_stats,
    relation_name,
    walk_plans,
    psql_sql,
)
from workloads import scan_workloads


def run_prelude() -> None:
    path = os.path.join(os.path.dirname(__file__), "session_prelude.sql")
    with open(path, encoding="utf-8") as f:
        psql_sql(f.read())


def seq_scan_samples(root: dict) -> list:
    plan = root["Plan"]
    out = []
    for node in walk_plans(plan):
        if node.get("Node Type") == "Seq Scan":
            rel = relation_name(node)
            ex = exclusive_total_time(node)
            out.append(
                {
                    "node_type": "Seq Scan",
                    "relation": rel,
                    "exclusive_ms": ex,
                    "plan_rows": float(node.get("Plan Rows") or 0),
                    "actual_rows": float(node.get("Actual Rows") or 0),
                    "plan_width": int(node.get("Plan Width") or 0),
                    "shared_hit_blocks": float(node.get("Shared Hit Blocks") or 0),
                    "shared_read_blocks": float(node.get("Shared Read Blocks") or 0),
                }
            )
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.path.join(os.path.dirname(__file__), "data", "scan_samples.jsonl"))
    ap.add_argument("--repeats", type=int, default=1, help="median of N ANALYZE runs (large grids: keep 1)")
    ap.add_argument("--target", type=int, default=55, help="number of distinct SQL workloads")
    ap.add_argument("--limit", type=int, default=0, help="only first N workloads (debug)")
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    stats = load_table_stats()
    workloads = scan_workloads(args.target)
    if args.limit:
        workloads = workloads[: args.limit]

    n_ok = 0
    with open(args.out, "w", encoding="utf-8") as fout:
        for tag, sql in workloads:
            run_prelude()
            try:
                explain_analyze_json(sql)
            except Exception as e:
                print(f"[skip warmup] {tag}: {e}", file=sys.stderr)

            times_runs: list[list[dict]] = []
            ok = False
            for _ in range(args.repeats):
                run_prelude()
                try:
                    root = explain_analyze_json(sql)
                    samples = seq_scan_samples(root)
                except Exception as e:
                    print(f"[fail] {tag}: {e}", file=sys.stderr)
                    samples = []
                if not samples:
                    continue
                ok = True
                times_runs.append(samples)

            if not ok:
                continue

            n = len(times_runs[0])
            if n != 1:
                print(f"[warn] {tag}: expected 1 Seq Scan, got {n}", file=sys.stderr)
            idx = 0
            rel = times_runs[0][idx]["relation"]
            if not rel or rel not in stats:
                print(f"[warn] {tag}: unknown relation {rel}", file=sys.stderr)
                continue
            tuples, pages = stats[rel]
            ex_vals = [float(r[idx]["exclusive_ms"]) for r in times_runs if idx < len(r)]
            ex = float(statistics.median(ex_vals))
            row = {
                "tag": tag,
                "sql": sql,
                "relation": rel,
                "reltuples": tuples,
                "relpages": pages,
                "exclusive_ms": ex,
                "plan_rows": float(times_runs[0][idx]["plan_rows"]),
                "actual_rows": float(times_runs[0][idx]["actual_rows"]),
                "plan_width": int(times_runs[0][idx]["plan_width"]),
                "shared_hit_blocks": float(
                    statistics.median([float(r[idx]["shared_hit_blocks"]) for r in times_runs])
                ),
                "shared_read_blocks": float(
                    statistics.median([float(r[idx]["shared_read_blocks"]) for r in times_runs])
                ),
            }
            fout.write(json.dumps(row, ensure_ascii=False) + "\n")
            n_ok += 1
            print(tag, "->", ex, "ms")

    print("wrote", args.out, "count", n_ok)
    if n_ok < 50:
        print(f"[warn] only {n_ok} samples (<50). Increase --target or fix failures.", file=sys.stderr)


if __name__ == "__main__":
    main()
