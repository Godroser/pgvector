#!/usr/bin/env python3
"""
Collect Index Scan / Index Only Scan (same GUC as 03_collect_index_scan.py).

cost-ascii.md §3 / §3.1 features and substitutes:

  P_idx : index pages touched — not known exactly without planner internals.
          SUBSTITUTE: P_idx_access_est = ceil(N_idx * index_relpages / index_reltuples)
          with N_idx ~ Plan Rows (proxy for rows/index entries processed; same role as
          clamped estimate from selectivity * N_heap in genericcostestimate).

  N_heap, N_fetch : baserel tuples and heap tuples fetched.
          N_heap from pg_class (heap rel).
          SUBSTITUTE for N_fetch: Plan Rows on the scan node (downstream: plan-time
          estimate of rows / fetches for simple index paths).

  R_out, T_pt * R_out : Plan Rows and projection — SUBSTITUTE proj_proxy_plan_row_bytes
          = Plan Rows * Plan Width.

  Index + qpquals: indexTotal adds N_idx * (C_idx + C_op * (n_q + n_o)); heap side
          adds (C_tuple + Qqp_pt) * N_fetch.
          SUBSTITUTE: N_fetch_times_qpqual_proxy = N_fetch_plan * (conjuncts(Index Cond)
          + conjuncts(Filter) + conjuncts(Recheck Cond)) via rough AND-counts in EXPLAIN
          strings or list length (see fit_common.explain_qual_conjuncts).

  indexStartup, Q_arg, Mackert-Lohman heap random pages: not exported per-field.
          Absorbed by linear intercept + P_idx / N_heap / N_fetch overlap.

  indexCorrelation rho: usually 0 in generic estimate; not modeled separately.

Output: data/index_scan_samples_new.jsonl
"""

from __future__ import annotations

import argparse
import json
import math
import os
import statistics
import sys

from fit_common import (
    exclusive_total_time,
    explain_analyze_json,
    explain_qual_conjuncts,
    load_index_stats,
    load_table_stats,
    walk_plans,
    psql_sql,
)
from workloads import index_scan_workloads

FORCE_INDEX = "SET LOCAL enable_seqscan TO off;"


def run_prelude() -> None:
    path = os.path.join(os.path.dirname(__file__), "session_prelude.sql")
    with open(path, encoding="utf-8") as f:
        psql_sql(f.read())


def index_scan_nodes(root: dict) -> list:
    plan = root["Plan"]
    out = []
    for node in walk_plans(plan):
        nt = node.get("Node Type")
        if nt in ("Index Scan", "Index Only Scan", "Bitmap Index Scan"):
            rel = (node.get("Relation Name") or "").lower()
            idx_name = (node.get("Index Name") or "").lower()
            plan_rows = float(node.get("Plan Rows") or 0)
            plan_width = int(node.get("Plan Width") or 0)
            qpqual = (
                explain_qual_conjuncts(node.get("Index Cond"))
                + explain_qual_conjuncts(node.get("Filter"))
                + explain_qual_conjuncts(node.get("Recheck Cond"))
            )
            out.append(
                {
                    "node_type": nt,
                    "relation": rel,
                    "index_name": idx_name,
                    "alias": node.get("Alias"),
                    "exclusive_ms": exclusive_total_time(node),
                    "plan_rows": plan_rows,
                    "actual_rows": float(node.get("Actual Rows") or 0),
                    "plan_width": plan_width,
                    "qpqual_proxy_conjuncts": qpqual,
                }
            )
    return out


def p_idx_access_est(plan_rows: float, idx_tuples: float, idx_pages: float) -> float:
    n_idx = max(1.0, plan_rows)
    if idx_tuples > 1.0:
        n_idx = min(n_idx, idx_tuples)
        return float(math.ceil(n_idx * idx_pages / max(idx_tuples, 1.0)))
    if idx_pages > 0.0:
        return 1.0
    return 1.0


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--out",
        default=os.path.join(os.path.dirname(__file__), "data", "index_scan_samples_new.jsonl"),
    )
    ap.add_argument("--repeats", type=int, default=1)
    ap.add_argument("--target", type=int, default=55)
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    stats = load_table_stats()
    idx_stats = load_index_stats()
    workloads = index_scan_workloads(args.target)
    if args.limit:
        workloads = workloads[: args.limit]

    n_ok = 0
    with open(args.out, "w", encoding="utf-8") as fout:
        for tag, sql in workloads:
            runs = []
            for _ in range(args.repeats):
                run_prelude()
                try:
                    root = explain_analyze_json(sql, tx_prefix=FORCE_INDEX)
                except Exception as e:
                    print(f"[fail] {tag}: {e}", file=sys.stderr)
                    continue
                runs.append(index_scan_nodes(root))
            if not runs:
                continue
            cand = None
            for r in runs[0]:
                if r["node_type"] in ("Index Scan", "Index Only Scan"):
                    cand = r
                    break
            if not cand:
                print(f"[warn] {tag}: no Index Scan in plan", file=sys.stderr)
                continue
            rel = cand["relation"]
            idx_key = cand["index_name"]
            if rel not in stats:
                print(f"[warn] {tag}: bad relation", file=sys.stderr)
                continue
            if not idx_key or idx_key not in idx_stats:
                print(f"[warn] {tag}: unknown index {idx_key!r}", file=sys.stderr)
                continue

            def match(sample_list):
                for s in sample_list:
                    if s["node_type"] == cand["node_type"] and s["relation"] == rel:
                        return s
                return None

            vals = []
            for r in runs:
                m = match(r)
                if m:
                    vals.append(m["exclusive_ms"])
            if not vals:
                continue
            ex = float(statistics.median(vals))
            reltuples, relpages = stats[rel]
            idx_tuples, idx_pages = idx_stats[idx_key]
            pr = float(cand["plan_rows"])
            pw = int(cand["plan_width"])
            qp = float(cand["qpqual_proxy_conjuncts"])
            p_idx = p_idx_access_est(pr, idx_tuples, idx_pages)
            n_fetch_plan = pr
            row = {
                "tag": tag,
                "sql": sql,
                "relation": rel,
                "index_name": idx_key,
                "node_type": cand["node_type"],
                "exclusive_ms": ex,
                "P_idx_access_est": p_idx,
                "N_heap_tuples": reltuples,
                "P_heap_pages": relpages,
                "N_fetch_plan": n_fetch_plan,
                "R_out_plan": pr,
                "proj_proxy_plan_row_bytes": pr * float(pw),
                "N_fetch_times_qpqual_proxy": n_fetch_plan * qp,
                "qpqual_proxy_conjuncts": qp,
                "relpages": relpages,
                "reltuples": reltuples,
                "plan_rows": pr,
                "plan_width": pw,
                "actual_rows": float(cand["actual_rows"]),
            }
            fout.write(json.dumps(row, ensure_ascii=False) + "\n")
            n_ok += 1
            print(tag, ex, "ms")

    print("wrote", args.out, "count", n_ok)
    if n_ok < 50:
        print(f"[warn] only {n_ok} index-scan samples (<50).", file=sys.stderr)


if __name__ == "__main__":
    main()
