#!/usr/bin/env python3
"""
Merge Join samples (same GUC as 09_collect_mergejoin.py).

cost-ascii.md §5 (Merge Join side):

  J — rows passing mergequals; output CPU run += J * (C_tuple + Qqp_per_tuple) + T_pt * R_out.
          SUBSTITUTE: join Plan Rows for J; proj_proxy = Plan Rows * Plan Width.

  Outer/inner sizes drive merge comparison / rescan behaviour; not fully decomposed here.
          SUBSTITUTE: outer_plan_rows, inner_plan_rows from children Plan Rows;
          R_outer_times_k with k = join_qual_clause_count("Merge Cond") for R_O * k style probe cost.

  Sort/Materialize costs under the join are in child nodes (not merge-exclusive).

Output: data/mergejoin_samples_new.jsonl
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys

from fit_common import exclusive_total_time, explain_analyze_json, join_qual_clause_count, walk_plans, psql_sql
from workloads import mergejoin_workloads

MERGE_GUC = """SET LOCAL enable_hashjoin TO off;
SET LOCAL enable_nestloop TO off;
SET LOCAL enable_mergejoin TO on;"""


def run_prelude() -> None:
    path = os.path.join(os.path.dirname(__file__), "session_prelude.sql")
    with open(path, encoding="utf-8") as f:
        psql_sql(f.read())


def merge_join_row(root: dict):
    for node in walk_plans(root["Plan"]):
        if node.get("Node Type") == "Merge Join":
            kids = node.get("Plans") or []
            if len(kids) < 2:
                return None
            outer_plan = float(kids[0].get("Plan Rows") or 0)
            inner_plan = float(kids[1].get("Plan Rows") or 0)
            oa = float(kids[0].get("Actual Rows") or 0)
            ia = float(kids[1].get("Actual Rows") or 0)
            k = join_qual_clause_count(node, "Merge Cond")
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
                "merge_k": k,
                "R_outer_times_k": outer_plan * k,
                "R_inner_plan": inner_plan,
                "J_plan": pr,
                "proj_proxy_join_bytes": pr * float(pw),
            }
    return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--out",
        default=os.path.join(os.path.dirname(__file__), "data", "mergejoin_samples_new.jsonl"),
    )
    ap.add_argument("--repeats", type=int, default=1)
    ap.add_argument("--target", type=int, default=200)
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    workloads = mergejoin_workloads(args.target)
    if args.limit:
        workloads = workloads[: args.limit]

    n_ok = 0
    with open(args.out, "w", encoding="utf-8") as fout:
        for tag, sql in workloads:
            meds = []
            for _ in range(args.repeats):
                run_prelude()
                try:
                    root = explain_analyze_json(sql, tx_prefix=MERGE_GUC)
                except Exception as e:
                    print(f"[fail] {tag}: {e}", file=sys.stderr)
                    meds = []
                    break
                row = merge_join_row(root)
                if not row:
                    print(f"[warn] {tag}: no Merge Join under GUCs", file=sys.stderr)
                    meds = []
                    break
                meds.append(row)
            if not meds:
                continue
            keys_float = [
                k
                for k in meds[0]
                if k not in ("plan_width", "merge_k")
            ]
            out = {k: float(statistics.median([m[k] for m in meds])) for k in keys_float}
            out["plan_width"] = int(statistics.median([m["plan_width"] for m in meds]))
            out["merge_k"] = float(statistics.median([m["merge_k"] for m in meds]))
            out["tag"] = tag
            out["sql"] = sql
            fout.write(json.dumps(out, ensure_ascii=False) + "\n")
            n_ok += 1
            print(tag, out["exclusive_ms"], "ms")

    print("wrote", args.out, "count", n_ok)
    if n_ok < 200:
        print(f"[warn] only {n_ok} merge-join samples (<200).", file=sys.stderr)


if __name__ == "__main__":
    main()
