#!/usr/bin/env python3
"""
Example: subtract scan-only prediction from measured child times.

Uses models/scan_coef.json from 02_fit_scan.py and pg_class stats.
Useful when fitting join/agg on raw timings instead of EXCLUSIVE node times.

For EXCLUSIVE times from EXPLAIN (ANALYZE), child contributions are already
subtracted by the executor model; this script is optional diagnostics.
"""

from __future__ import annotations

import argparse
import json
import os

from fit_common import load_json, load_table_stats


def predict_scan_ms(coef_path: str, rel: str, actual_rows: float, plan_width: int) -> float:
    c = load_json(coef_path)
    stats = load_table_stats()
    rel = rel.lower()
    if rel not in stats:
        return 0.0
    tuples, pages = stats[rel]
    names = c["features"]
    coef = [c["intercept_ms"]] + [c["coef"][k] for k in names]
    x = [pages, tuples, actual_rows, plan_width]
    return coef[0] + sum(coef[i + 1] * x[i] for i in range(len(x)))


def main() -> None:
    ap = argparse.ArgumentParser(description="Demo scan prediction from fitted coefficients.")
    ap.add_argument("--coef", default=os.path.join(os.path.dirname(__file__), "models", "scan_coef.json"))
    ap.add_argument("--relation", required=True, help="e.g. lineitem")
    ap.add_argument("--actual-rows", type=float, required=True)
    ap.add_argument("--plan-width", type=int, default=8)
    args = ap.parse_args()

    ms = predict_scan_ms(args.coef, args.relation, args.actual_rows, args.plan_width)
    print(f"predicted_seq_scan_exclusive_ms ~ {ms:.4f}")


if __name__ == "__main__":
    main()
