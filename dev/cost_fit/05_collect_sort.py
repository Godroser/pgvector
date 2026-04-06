#!/usr/bin/env python3
"""Collect Sort node samples (>=55 workloads by default)."""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys

from fit_common import exclusive_total_time, explain_analyze_json, walk_plans
from workloads import sort_workloads


def sort_explain_prefix() -> str:
    """
    Session settings applied in the *same transaction* as EXPLAIN (required: each
    psql invocation is a new connection, so a separate run_prelude() does not affect
    EXPLAIN).

    Disable index paths so ORDER BY cannot use an index-ordered scan (e.g.
    idx_orders_orderdate on orders(o_orderdate) would skip a Sort node entirely).
    """
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
        # Incremental Sort (PG13+) is still a sort-style operator for cost fitting
        if nt not in ("Sort", "Incremental Sort"):
            continue
        sm = node.get("Sort Method") or ""
        ext = 1.0 if "external" in sm.lower() else 0.0
        out.append(
            {
                "exclusive_ms": exclusive_total_time(node),
                "plan_rows": float(node.get("Plan Rows") or 0),
                "actual_rows": float(node.get("Actual Rows") or 0),
                "plan_width": int(node.get("Plan Width") or 0),
                "sort_external": ext,
            }
        )
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.path.join(os.path.dirname(__file__), "data", "sort_samples.jsonl"))
    ap.add_argument("--repeats", type=int, default=1)
    ap.add_argument("--target", type=int, default=55)
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
            row = {
                "tag": tag,
                "sql": sql,
                "exclusive_ms": float(statistics.median([m["exclusive_ms"] for m in meds])),
                "plan_rows": float(statistics.median([m["plan_rows"] for m in meds])),
                "actual_rows": float(statistics.median([m["actual_rows"] for m in meds])),
                "plan_width": int(statistics.median([m["plan_width"] for m in meds])),
                "sort_external": float(statistics.median([m["sort_external"] for m in meds])),
            }
            fout.write(json.dumps(row, ensure_ascii=False) + "\n")
            n_ok += 1
            print(tag, row["exclusive_ms"], "ms")

    print("wrote", args.out, "count", n_ok)
    if n_ok < 50:
        print(f"[warn] only {n_ok} sort samples (<50).", file=sys.stderr)


if __name__ == "__main__":
    main()
