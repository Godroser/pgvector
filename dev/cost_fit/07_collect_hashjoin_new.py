#!/usr/bin/env python3
"""
Hash Join samples (same GUC as 07_collect_hashjoin.py).

cost-ascii.md §4:

  R_O, R_I — outer / inner row counts.
          SUBSTITUTE: Plan Rows on first child (outer) and second child (inner); keep
          *_actual_rows for debugging / legacy parity with old script.

  k — number of hash clauses.
          SUBSTITUTE: join_qual_clause_count("Hash Cond") — list length or AND-count.

  J / R_out — join output rows; Qqp, T_pt*R_out on output.
          SUBSTITUTE: join Plan Rows, proj_proxy_join_bytes = Plan Rows * Plan Width.

  P_I, P_O, numbatches — spill / batch pages.
          SUBSTITUTE: hash_spill_proxy = 1 if EXPLAIN "Hash Batches" > 1 else 0.
          hash_spill_rows_proxy = spill * (inner_plan + outer_plan) as crude multi-batch scale.

  f_bucket, inner_unique: not available in EXPLAIN JSON — intercept + linear terms absorb.

Output: data/hashjoin_samples_new.jsonl
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys

from fit_common import exclusive_total_time, explain_analyze_json, join_qual_clause_count, walk_plans, psql_sql
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
            outer_plan = float(kids[0].get("Plan Rows") or 0)
            inner_plan = float(kids[1].get("Plan Rows") or 0)
            oa = float(kids[0].get("Actual Rows") or 0)
            ia = float(kids[1].get("Actual Rows") or 0)
            k = join_qual_clause_count(node, "Hash Cond")
            batches = int(node.get("Hash Batches") or 1)
            spill = 1.0 if batches > 1 else 0.0
            pr = float(node.get("Plan Rows") or 0)
            pw = int(node.get("Plan Width") or 0)
            return {
                "exclusive_ms": exclusive_total_time(node),
                "plan_rows": pr,
                "actual_rows": float(node.get("Actual Rows") or 0),
                "plan_width": pw,
                "outer_plan_rows": outer_plan,
                "inner_plan_rows": inner_plan,
                "outer_actual_rows": oa,
                "inner_actual_rows": ia,
                "hash_k": k,
                "R_outer_times_k": outer_plan * k,
                "R_inner_plan": inner_plan,
                "J_plan": pr,
                "proj_proxy_join_bytes": pr * float(pw),
                "hash_spill_proxy": spill,
                "hash_spill_rows_proxy": spill * (outer_plan + inner_plan),
            }
    return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--out",
        default=os.path.join(os.path.dirname(__file__), "data", "hashjoin_samples_new.jsonl"),
    )
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
            keys_float = [
                k
                for k in meds[0]
                if k
                not in (
                    "plan_width",
                    "hash_spill_proxy",
                    "hash_k",
                )
            ]
            out = {k: float(statistics.median([m[k] for m in meds])) for k in keys_float}
            out["plan_width"] = int(statistics.median([m["plan_width"] for m in meds]))
            out["hash_spill_proxy"] = float(statistics.median([m["hash_spill_proxy"] for m in meds]))
            out["hash_k"] = float(statistics.median([m["hash_k"] for m in meds]))
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
