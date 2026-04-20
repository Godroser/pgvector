#!/usr/bin/env python3
"""
Concurrent-session sampling: for each operator workload, run N identical EXPLAIN ANALYZE
sessions in parallel and record the target node's exclusive time distribution.

Baseline `single_exclusive_ms` is read from sibling cost_fit/data/*_samples*.jsonl (same tag)
when available; use --measure-single to fill missing tags with one sequential EXPLAIN.

Output: data/<operator>_parallel_samples.jsonl

Does not modify dev/cost_fit.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import statistics
import sys
from typing import Any, Dict, List, Optional

from operator_parallel import OperatorSpec, get_operator_spec, list_operator_names
from parallel_common import (
    concurrent_explain_runs,
    explain_one,
    load_single_baselines,
    summarize_times,
)


def measure_exclusive(
    sql: str,
    tx_prefix: str,
    spec: OperatorSpec,
) -> Optional[float]:
    try:
        root = explain_one(sql, tx_prefix)
    except Exception as e:
        print(f"[fail] EXPLAIN: {e}", file=sys.stderr)
        return None
    return spec.extract(root)


def resolve_tx_prefix(spec: OperatorSpec, knobs: Dict[str, Any]) -> str:
    if spec.tx_prefix_needs_knobs:
        return spec.tx_prefix(knobs=knobs)
    return spec.tx_prefix()


def collect_for_tag_degree(
    sql: str,
    tx_prefix: str,
    spec: OperatorSpec,
    degree: int,
    rounds: int,
) -> Optional[Dict[str, Any]]:
    round_medians: List[float] = []
    all_vals: List[float] = []
    last_wall = 0.0
    for _r in range(rounds):

        def run_once() -> float:
            v = measure_exclusive(sql, tx_prefix, spec)
            if v is None:
                raise RuntimeError("extract returned None")
            return float(v)

        try:
            vals, wall_ms = concurrent_explain_runs(degree, run_once)
        except Exception as e:
            print(f"[fail] concurrent degree={degree}: {e}", file=sys.stderr)
            return None
        last_wall = wall_ms
        nums = [float(x) for x in vals]
        all_vals.extend(nums)
        sm = summarize_times(nums)
        round_medians.append(sm["median_ms"])

    agg_med = float(statistics.median(round_medians)) if round_medians else float("nan")
    overall = summarize_times(all_vals)
    return {
        "parallel_median_of_round_medians_ms": agg_med,
        "parallel_round_median_ms": round_medians,
        "parallel_exclusive_ms_all_runs": all_vals,
        "parallel_summarize_all_runs": overall,
        "wall_clock_last_batch_ms": last_wall,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Collect parallel EXCLUSIVE times per operator.")
    ap.add_argument(
        "--operator",
        required=True,
        choices=list_operator_names(),
        help="Operator family aligned with cost_fit collectors",
    )
    ap.add_argument(
        "--out",
        default="",
        help="Output jsonl (default: data/<operator>_parallel_samples.jsonl under this dir)",
    )
    ap.add_argument("--target", type=int, default=200, help="Workload count from workloads.py")
    ap.add_argument("--limit", type=int, default=0, help="Only first N workloads")
    ap.add_argument(
        "--degrees",
        default="2,4,8,16",
        help="Comma-separated concurrent session counts (e.g. 2,4,8,16)",
    )
    ap.add_argument(
        "--rounds",
        type=int,
        default=1,
        help="Repeat each (tag,degree) concurrent batch; aggregate medians across rounds",
    )
    ap.add_argument(
        "--single-jsonl",
        default="",
        help="Override path to cost_fit single-run jsonl (default: operator's data_file)",
    )
    ap.add_argument(
        "--measure-single",
        action="store_true",
        help="If tag missing in single-jsonl, run one sequential EXPLAIN to get baseline",
    )
    ap.add_argument(
        "--warmup",
        action="store_true",
        help="One sequential EXPLAIN per workload before sampling (buffer priming)",
    )
    args = ap.parse_args()

    spec = get_operator_spec(args.operator)
    base_dir = os.path.dirname(os.path.abspath(__file__))
    out_path = args.out or os.path.join(base_dir, "data", f"{spec.name}_parallel_samples.jsonl")
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)

    single_path = args.single_jsonl or spec.data_file
    baselines = load_single_baselines(single_path)

    degrees: List[int] = []
    for part in args.degrees.split(","):
        part = part.strip()
        if not part:
            continue
        degrees.append(int(part))
    if not degrees or any(d < 1 for d in degrees):
        raise SystemExit("degrees must be non-empty and all >= 1")

    raw_wl = spec.workloads(args.target)
    if args.limit:
        raw_wl = raw_wl[: args.limit]

    n_out = 0
    with open(out_path, "w", encoding="utf-8") as fout:
        for tag, sql, knobs in raw_wl:
            print(f"[progress] workload tag={tag!r} (single + degrees {degrees})...", flush=True)
            tx_prefix = resolve_tx_prefix(spec, knobs)

            single_ms: Optional[float] = baselines.get(tag)
            single_src = "cost_fit_jsonl"
            if single_ms is None and args.measure_single:
                print(f"[progress] measure-single EXPLAIN for {tag!r} ...", flush=True)
                v = measure_exclusive(sql, tx_prefix, spec)
                if v is not None:
                    single_ms = float(v)
                    single_src = "measured_sequential"
                else:
                    single_src = "missing"
            elif single_ms is not None:
                single_ms = float(single_ms)

            if args.warmup:
                try:
                    explain_one(sql, tx_prefix)
                except Exception as e:
                    print(f"[warn] warmup {tag}: {e}", file=sys.stderr)

            for degree in degrees:
                print(f"[progress] concurrent degree={degree} for {tag!r} ...", flush=True)
                par = collect_for_tag_degree(sql, tx_prefix, spec, degree, args.rounds)
                if not par:
                    continue
                row: Dict[str, Any] = {
                    "operator": spec.name,
                    "tag": tag,
                    "sql": sql,
                    "knobs": knobs,
                    "degree": degree,
                    "single_exclusive_ms": single_ms,
                    "single_source": single_src,
                    "rounds": args.rounds,
                    **par,
                }
                pm = par["parallel_median_of_round_medians_ms"]
                if single_ms is not None and single_ms > 1e-12 and not math.isnan(pm):
                    row["parallel_median_ms"] = pm
                    row["slowdown_ratio_median"] = float(pm) / float(single_ms)
                else:
                    row["parallel_median_ms"] = pm
                    row["slowdown_ratio_median"] = None

                fout.write(json.dumps(row, ensure_ascii=False) + "\n")
                n_out += 1
                print(tag, "d=", degree, "parallel_median_ms=", pm, "single=", single_ms, flush=True)

    print("wrote", out_path, "rows", n_out, flush=True)


if __name__ == "__main__":
    main()
