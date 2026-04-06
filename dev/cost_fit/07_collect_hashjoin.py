#!/usr/bin/env python3
"""
Hash Join samples. SET LOCAL disables nest loop / mergejoin to prefer hash join.
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys

from fit_common import exclusive_total_time, explain_analyze_json, walk_plans, psql_sql
from workloads import hashjoin_workloads

HASH_FORCE = """SET LOCAL enable_nestloop TO off;
SET LOCAL enable_mergejoin TO off;"""


def run_prelude() -> None:
    path = os.path.join(os.path.dirname(__file__), "session_prelude.sql")
    with open(path, encoding="utf-8") as f:
        psql_sql(f.read())


def hash_join_row(root: dict):
    for node in walk_plans(root["Plan"]):
        if node.get("Node Type") == "Hash Join":
            kids = node.get("Plans") or []
            if len(kids) < 2:
                return None
            oa = float(kids[0].get("Actual Rows") or 0)
            ia = float(kids[1].get("Actual Rows") or 0)
            return {
                "exclusive_ms": exclusive_total_time(node),
                "plan_rows": float(node.get("Plan Rows") or 0),
                "actual_rows": float(node.get("Actual Rows") or 0),
                "plan_width": int(node.get("Plan Width") or 0),
                "outer_actual_rows": oa,
                "inner_actual_rows": ia,
            }
    return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.path.join(os.path.dirname(__file__), "data", "hashjoin_samples.jsonl"))
    ap.add_argument("--repeats", type=int, default=1)
    ap.add_argument("--target", type=int, default=55)
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    workloads = hashjoin_workloads(args.target)
    if args.limit:
        workloads = workloads[: args.limit]

    n_ok = 0
    with open(args.out, "w", encoding="utf-8") as fout:
        for tag, sql in workloads:
            meds = []
            for _ in range(args.repeats):
                run_prelude()
                try:
                    root = explain_analyze_json(sql, tx_prefix=HASH_FORCE)
                except Exception as e:
                    print(f"[fail] {tag}: {e}", file=sys.stderr)
                    meds = []
                    break
                row = hash_join_row(root)
                if not row:
                    print(f"[warn] {tag}: no Hash Join", file=sys.stderr)
                    meds = []
                    break
                meds.append(row)
            if not meds:
                continue
            out = {k: float(statistics.median([m[k] for m in meds])) for k in meds[0] if k != "plan_width"}
            out["plan_width"] = int(statistics.median([m["plan_width"] for m in meds]))
            out["tag"] = tag
            out["sql"] = sql
            fout.write(json.dumps(out, ensure_ascii=False) + "\n")
            n_ok += 1
            print(tag, out["exclusive_ms"], "ms")

    print("wrote", args.out, "count", n_ok)
    if n_ok < 50:
        print(f"[warn] only {n_ok} hash-join samples (<50).", file=sys.stderr)


if __name__ == "__main__":
    main()
