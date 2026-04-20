#!/usr/bin/env python3
"""
Linear regression: parallel median exclusive time ~ f(single_exclusive_ms, degree).

Default features:
  - single_exclusive_ms
  - degree
  - single_times_log2_degree = single_exclusive_ms * log2(max(degree, 2))

Input: data/<operator>_parallel_samples.jsonl from 01_collect_parallel.py
Output: models/<operator>_parallel_coef.json

Uses dev/cost_fit/fit_common.py (lstsq + train/test metrics) — does not modify cost_fit.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from typing import Any, Dict, List

# fit_common lives in ../cost_fit
_COST_FIT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "cost_fit"))
if _COST_FIT not in sys.path:
    sys.path.insert(0, _COST_FIT)

from fit_common import fit_train_test_eval, save_json  # noqa: E402

from operator_parallel import list_operator_names  # noqa: E402


def load_rows(path: str) -> List[dict]:
    rows: List[dict] = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def main() -> None:
    ap = argparse.ArgumentParser(description="Fit parallel exclusive time from single + degree.")
    ap.add_argument("--operator", required=True, choices=list_operator_names())
    ap.add_argument(
        "--data",
        default="",
        help="Parallel samples jsonl (default: data/<operator>_parallel_samples.jsonl)",
    )
    ap.add_argument(
        "--out",
        default="",
        help="Output coef JSON (default: models/<operator>_parallel_coef.json)",
    )
    ap.add_argument("--train-n", type=int, default=80)
    ap.add_argument("--test-n", type=int, default=20)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    base = os.path.dirname(os.path.abspath(__file__))
    data_path = args.data or os.path.join(base, "data", f"{args.operator}_parallel_samples.jsonl")
    out_path = args.out or os.path.join(base, "models", f"{args.operator}_parallel_coef.json")

    rows_in = load_rows(data_path)
    rows: List[dict] = []
    for r in rows_in:
        if r.get("operator") != args.operator:
            continue
        s = r.get("single_exclusive_ms")
        pm = r.get("parallel_median_ms")
        if s is None or pm is None:
            continue
        try:
            sf = float(s)
            pf = float(pm)
        except (TypeError, ValueError):
            continue
        if math.isnan(sf) or math.isnan(pf) or sf < 0:
            continue
        rows.append(r)

    need = args.train_n + args.test_n
    if len(rows) < need:
        raise SystemExit(
            f"Need at least {need} usable rows (single + parallel median), got {len(rows)}. "
            f"Run 01_collect_parallel.py with --measure-single if baselines are missing."
        )

    names = ["single_exclusive_ms", "degree", "single_times_log2_degree"]

    def xfn(r: dict) -> List[float]:
        s = float(r["single_exclusive_ms"])
        d = float(r["degree"])
        lg = math.log2(max(d, 2.0))
        return [s, d, s * lg]

    ev = fit_train_test_eval(
        rows,
        names,
        xfn,
        y_key="parallel_median_ms",
        train_n=args.train_n,
        test_n=args.test_n,
        seed=args.seed,
    )

    payload: Dict[str, Any] = {
        "operator": args.operator,
        "model": "parallel_exclusive_ms ~ linear(single_exclusive_ms, degree, single*log2(max(degree,2)))",
        "features": names,
        "feature_notes": {
            "single_exclusive_ms": "Baseline exclusive time from solo run (cost_fit jsonl or measured)",
            "degree": "Number of concurrent identical EXPLAIN ANALYZE sessions",
            "single_times_log2_degree": "Interaction term: single * log2(max(degree,2))",
            "intercept": "Fixed overhead / contention not explained by linear terms",
        },
        "data_path": os.path.abspath(data_path),
        "total_samples": len(rows),
        **{k: v for k, v in ev.items() if k != "test_predictions"},
    }

    pred_path = os.path.join(os.path.dirname(out_path) or ".", f"{args.operator}_parallel_test_predictions.jsonl")
    os.makedirs(os.path.dirname(pred_path) or ".", exist_ok=True)
    with open(pred_path, "w", encoding="utf-8") as pf:
        for item in ev.get("test_predictions", []):
            pf.write(json.dumps(item, ensure_ascii=False) + "\n")

    save_json(out_path, payload)
    print(json.dumps(payload, indent=2))
    print("wrote", out_path, pred_path)


if __name__ == "__main__":
    main()
