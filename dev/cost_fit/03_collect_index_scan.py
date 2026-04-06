#!/usr/bin/env python3
"""
Collect Index Scan / Index Only Scan (SET LOCAL enable_seqscan = off).
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys

from fit_common import exclusive_total_time, explain_analyze_json, load_table_stats, walk_plans, psql_sql
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
            alias = node.get("Alias")
            out.append(
                {
                    "node_type": nt,
                    "relation": rel,
                    "alias": alias,
                    "exclusive_ms": exclusive_total_time(node),
                    "plan_rows": float(node.get("Plan Rows") or 0),
                    "actual_rows": float(node.get("Actual Rows") or 0),
                    "plan_width": int(node.get("Plan Width") or 0),
                }
            )
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.path.join(os.path.dirname(__file__), "data", "index_scan_samples.jsonl"))
    ap.add_argument("--repeats", type=int, default=1)
    ap.add_argument("--target", type=int, default=55)
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    stats = load_table_stats()
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
            if rel not in stats:
                print(f"[warn] {tag}: bad relation", file=sys.stderr)
                continue
            tuples, pages = stats[rel]

            def match_score(sample_list):
                for s in sample_list:
                    if s["node_type"] == cand["node_type"] and s["relation"] == rel:
                        return s["exclusive_ms"]
                return None

            vals = [match_score(r) for r in runs]
            vals = [v for v in vals if v is not None]
            if not vals:
                continue
            ex = float(statistics.median(vals))
            row = {
                "tag": tag,
                "sql": sql,
                "relation": rel,
                "node_type": cand["node_type"],
                "relpages": pages,
                "reltuples": tuples,
                "exclusive_ms": ex,
                "plan_rows": cand["plan_rows"],
                "actual_rows": cand["actual_rows"],
                "plan_width": cand["plan_width"],
            }
            fout.write(json.dumps(row, ensure_ascii=False) + "\n")
            n_ok += 1
            print(tag, ex, "ms")

    print("wrote", args.out, "count", n_ok)
    if n_ok < 50:
        print(f"[warn] only {n_ok} index-scan samples (<50).", file=sys.stderr)


if __name__ == "__main__":
    main()
