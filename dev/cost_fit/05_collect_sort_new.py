#!/usr/bin/env python3
"""
Collect Sort / Incremental Sort (same session prefix as 05_collect_sort.py).

cost-ascii.md §5 (Sort / cost_tuplesort):

  In-memory: sort_startup ~ C_cmp * N * log2(N), sort_run ~ C_op * N (N = input rows).
          SUBSTITUTE N: Plan Rows on Sort node (downstream: planner estimate).
          Features: N_sort_logn = N * log2(max(N,2)) and N_sort_plan = N.

  External sort: extra I/O ~ f(N_pages, runs, mergeorder) with disk page terms.
          SUBSTITUTE: sort_external = 1 if Sort Method contains 'external', else 0;
          N_sort_spill_proxy_rows = N * sort_external (crude scale for spill with input size).

  Tuple width / memory pressure: not a separate symbol in the ASCII summary.
          SUBSTITUTE: sort_tuple_bytes_proxy = Plan Rows * Plan Width (bytes-like scale).

  Projection / input path cost: wrapped in child node times; Sort exclusive fits the
          sort-specific terms above plus intercept slop.

Output: data/sort_samples_new.jsonl
"""

from __future__ import annotations

import argparse
import json
import math
import os
import statistics
import sys

from fit_common import exclusive_total_time, explain_analyze_json, walk_plans
from workloads import sort_workloads


def sort_explain_prefix() -> str:
    path = os.path.join(os.path.dirname(__file__), "session_prelude.sql")
    with open(path, encoding="utf-8") as f:
        prelude = f.read().strip()
    return (
        prelude
        + "\nSET LOCAL enable_indexscan TO off;\n"
        "SET LOCAL enable_bitmapscan TO off;\n"
        "SET LOCAL enable_indexonlyscan TO off;\n"
    )


def sort_samples(root: dict) -> list:
    plan = root["Plan"]
    out = []
    for node in walk_plans(plan):
        nt = node.get("Node Type")
        if nt not in ("Sort", "Incremental Sort"):
            continue
        sm = node.get("Sort Method") or ""
        ext = 1.0 if "external" in sm.lower() else 0.0
        pr = float(node.get("Plan Rows") or 0)
        pw = int(node.get("Plan Width") or 0)
        n = max(pr, 2.0)  # doc: input count forced to at least 2 before log
        n_logn = pr * math.log2(n)
        out.append(
            {
                "exclusive_ms": exclusive_total_time(node),
                "plan_rows": pr,
                "actual_rows": float(node.get("Actual Rows") or 0),
                "plan_width": pw,
                "sort_external": ext,
                "N_sort_logn": n_logn,
                "N_sort_plan": pr,
                "N_sort_spill_proxy_rows": pr * ext,
                "sort_tuple_bytes_proxy": pr * float(pw),
            }
        )
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--out",
        default=os.path.join(os.path.dirname(__file__), "data", "sort_samples_new.jsonl"),
    )
    ap.add_argument("--repeats", type=int, default=1)
    ap.add_argument("--target", type=int, default=200)
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    workloads = sort_workloads(args.target)
    if args.limit:
        workloads = workloads[: args.limit]

    prefix = sort_explain_prefix()
    n_ok = 0
    with open(args.out, "w", encoding="utf-8") as fout:
        for tag, sql in workloads:
            meds = []
            for _ in range(args.repeats):
                try:
                    root = explain_analyze_json(sql, tx_prefix=prefix)
                except Exception as e:
                    print(f"[fail] {tag}: {e}", file=sys.stderr)
                    meds = []
                    break
                ss = sort_samples(root)
                if not ss:
                    print(f"[warn] {tag}: no Sort / Incremental Sort node", file=sys.stderr)
                    meds = []
                    break
                meds.append(ss[0])
            if not meds:
                continue
            m0 = meds[0]
            row = {
                "tag": tag,
                "sql": sql,
                "exclusive_ms": float(statistics.median([m["exclusive_ms"] for m in meds])),
                "plan_rows": float(statistics.median([m["plan_rows"] for m in meds])),
                "actual_rows": float(statistics.median([m["actual_rows"] for m in meds])),
                "plan_width": int(statistics.median([m["plan_width"] for m in meds])),
                "sort_external": float(statistics.median([m["sort_external"] for m in meds])),
                "N_sort_logn": float(statistics.median([m["N_sort_logn"] for m in meds])),
                "N_sort_plan": float(statistics.median([m["N_sort_plan"] for m in meds])),
                "N_sort_spill_proxy_rows": float(statistics.median([m["N_sort_spill_proxy_rows"] for m in meds])),
                "sort_tuple_bytes_proxy": float(statistics.median([m["sort_tuple_bytes_proxy"] for m in meds])),
            }
            fout.write(json.dumps(row, ensure_ascii=False) + "\n")
            n_ok += 1
            print(tag, row["exclusive_ms"], "ms")

    print("wrote", args.out, "count", n_ok)
    if n_ok < 200:
        print(f"[warn] only {n_ok} sort samples (<200).", file=sys.stderr)


if __name__ == "__main__":
    main()
