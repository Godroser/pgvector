#!/usr/bin/env python3
"""Aggregate / Hash Aggregate / GroupAggregate samples (>=55 workloads)."""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys

from fit_common import exclusive_total_time, explain_analyze_json, walk_plans, psql_sql
from workloads import agg_workloads


def run_prelude() -> None:
    path = os.path.join(os.path.dirname(__file__), "session_prelude.sql")
    with open(path, encoding="utf-8") as f:
        psql_sql(f.read())


AGG_TYPES = frozenset({"Hash Aggregate", "GroupAggregate", "Aggregate"})


def aggregate_row(root: dict):
    plan = root["Plan"]
    if plan.get("Node Type") in AGG_TYPES and "Partial" not in (plan.get("Node Type") or ""):
        return _row(plan)
    for node in walk_plans(plan):
        nt = node.get("Node Type") or ""
        if nt in AGG_TYPES and "Partial" not in nt:
            return _row(node)
    return None


def _row(node: dict) -> dict:
    kids = node.get("Plans") or []
    child_rows = float(kids[0].get("Actual Rows") or 0) if kids else 0.0
    gk = node.get("Group Key") or []
    return {
        "node_type": node.get("Node Type"),
        "exclusive_ms": exclusive_total_time(node),
        "plan_rows": float(node.get("Plan Rows") or 0),
        "actual_rows": float(node.get("Actual Rows") or 0),
        "plan_width": int(node.get("Plan Width") or 0),
        "child_actual_rows": child_rows,
        "num_group_keys": len(gk),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.path.join(os.path.dirname(__file__), "data", "agg_samples.jsonl"))
    ap.add_argument("--repeats", type=int, default=1)
    ap.add_argument("--target", type=int, default=55)
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    workloads = agg_workloads(args.target)
    if args.limit:
        workloads = workloads[: args.limit]

    n_ok = 0
    with open(args.out, "w", encoding="utf-8") as fout:
        for tag, sql in workloads:
            meds = []
            for _ in range(args.repeats):
                run_prelude()
                try:
                    root = explain_analyze_json(sql)
                except Exception as e:
                    print(f"[fail] {tag}: {e}", file=sys.stderr)
                    meds = []
                    break
                row = aggregate_row(root)
                if not row:
                    print(f"[warn] {tag}: no Aggregate node", file=sys.stderr)
                    meds = []
                    break
                meds.append(row)
            if not meds:
                continue
            out = {
                "tag": tag,
                "sql": sql,
                "node_type": meds[0]["node_type"],
                "exclusive_ms": float(statistics.median([m["exclusive_ms"] for m in meds])),
                "plan_rows": float(statistics.median([m["plan_rows"] for m in meds])),
                "actual_rows": float(statistics.median([m["actual_rows"] for m in meds])),
                "plan_width": int(statistics.median([m["plan_width"] for m in meds])),
                "child_actual_rows": float(statistics.median([m["child_actual_rows"] for m in meds])),
                "num_group_keys": int(statistics.median([m["num_group_keys"] for m in meds])),
            }
            fout.write(json.dumps(out, ensure_ascii=False) + "\n")
            n_ok += 1
            print(tag, out["exclusive_ms"], "ms", out["node_type"])

    print("wrote", args.out, "count", n_ok)
    if n_ok < 50:
        print(f"[warn] only {n_ok} agg samples (<50).", file=sys.stderr)


if __name__ == "__main__":
    main()
