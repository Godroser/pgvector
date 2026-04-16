#!/usr/bin/env python3
"""
Collect Seq Scan samples aligned with cost-ascii.md §2 (Seq Scan + filter + projection).

cost-ascii.md symbols:
  P        = baserel->pages
  N        = baserel->tuples (heap tuples scanned)
  R_out    = estimated output rows after filter
  Qs_su, Qs_pt = restriction qual startup / per_tuple (not exposed in EXPLAIN)
  T_su, T_pt = pathtarget projection startup / per_tuple (not exposed in EXPLAIN)

Downstream-available substitutes (see emitted JSON field names):
  R_out           -> R_out_plan: EXPLAIN "Plan Rows" (planner estimate; same role as R_out).
  T_pt * R_out    -> proj_proxy_plan_row_bytes: Plan Rows * Plan Width (bytes/row est * rows).
  Qs_pt side      -> N_times_qual_proxy: N_heap_tuples * qual_proxy_and_clauses, where
                    qual_proxy_and_clauses counts top-level " AND " segments in the
                    "Filter" text (0 if no Filter). Crude proxy for extra per-tuple qual CPU.
  Qs_su + T_su    -> not separately recorded; leave to linear intercept in 02_fit_scan_new.py.

  N_heap_tuples uses pg_class.reltuples for the scanned relation. Substitute: assumes a
  full-relation Seq Scan (N equals catalog tuple count). Partial scans would need a
  planner-estimated scan fraction (not wired here).

Original 01_collect_scan.py is unchanged; default output: data/scan_samples_new.jsonl
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


def qual_proxy_and_clauses(node: dict) -> float:
    """Rough count of conjuncts from EXPLAIN Filter text; 0 if absent."""
    f = node.get("Filter")
    if not f:
        return 0.0
    s = str(f)
    return float(s.count(" AND ") + 1)


def seq_scan_samples(root: dict) -> list:
    plan = root["Plan"]
    out = []
    for node in walk_plans(plan):
        if node.get("Node Type") == "Seq Scan":
            rel = relation_name(node)
            ex = exclusive_total_time(node)
            pr = float(node.get("Plan Rows") or 0)
            pw = int(node.get("Plan Width") or 0)
            quals = qual_proxy_and_clauses(node)
            out.append(
                {
                    "node_type": "Seq Scan",
                    "relation": rel,
                    "exclusive_ms": ex,
                    "plan_rows": pr,
                    "actual_rows": float(node.get("Actual Rows") or 0),
                    "plan_width": pw,
                    "qual_proxy_and_clauses": quals,
                    "shared_hit_blocks": float(node.get("Shared Hit Blocks") or 0),
                    "shared_read_blocks": float(node.get("Shared Read Blocks") or 0),
                }
            )
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--out",
        default=os.path.join(os.path.dirname(__file__), "data", "scan_samples_new.jsonl"),
    )
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
            reltuples, relpages = stats[rel]
            ex_vals = [float(r[idx]["exclusive_ms"]) for r in times_runs if idx < len(r)]
            ex = float(statistics.median(ex_vals))
            plan_rows = float(times_runs[0][idx]["plan_rows"])
            plan_width = int(times_runs[0][idx]["plan_width"])
            quals = float(times_runs[0][idx]["qual_proxy_and_clauses"])
            row = {
                "tag": tag,
                "sql": sql,
                "relation": rel,
                "exclusive_ms": ex,
                # §2: P, N, R_out + substitutes (see module docstring)
                "P_pages": relpages,
                "N_heap_tuples": reltuples,
                "R_out_plan": plan_rows,
                "proj_proxy_plan_row_bytes": plan_rows * float(plan_width),
                "N_times_qual_proxy": reltuples * quals,
                "qual_proxy_and_clauses": quals,
                # Legacy / debugging (same as 01_collect_scan.py naming)
                "relpages": relpages,
                "reltuples": reltuples,
                "plan_rows": plan_rows,
                "plan_width": plan_width,
                "actual_rows": float(times_runs[0][idx]["actual_rows"]),
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
