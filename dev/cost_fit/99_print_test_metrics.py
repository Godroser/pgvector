#!/usr/bin/env python3
"""Print test-set metrics from models/*_coef.json (after running fit scripts)."""

from __future__ import annotations

import json
import os
from typing import Any


def _fmt(x: Any, spec: str) -> str:
    """Format a numeric metric; missing or invalid values print as N/A (no crash on old coef JSON)."""
    if x is None:
        return "N/A"
    try:
        return format(float(x), spec)
    except (TypeError, ValueError):
        return str(x)


def main() -> None:
    base = os.path.join(os.path.dirname(__file__), "models")
    for name in (
        "scan_coef.json",
        "index_scan_coef.json",
        "hnsw_partition_scan_coef.json",
        "ivf_scan_coef.json",
        "sort_coef.json",
        "hashjoin_coef.json",
        "mergejoin_coef.json",
        "agg_coef.json",
    ):
        path = os.path.join(base, name)
        if not os.path.isfile(path):
            print(name, "— missing")
            continue
        with open(path, encoding="utf-8") as f:
            d = json.load(f)
        op = d.get("operator", name)
        print(f"=== {op} ===")
        ts, tn, ten = d.get("total_samples"), d.get("train_n"), d.get("test_n")
        print(
            f"  samples: {ts if ts is not None else 'N/A'}  "
            f"train: {tn if tn is not None else 'N/A'}  "
            f"test: {ten if ten is not None else 'N/A'}"
        )
        # legacy single-split files may only have rmse_ms / n
        if d.get("train_rmse_ms") is None and d.get("rmse_ms") is not None:
            print(
                f"  (legacy) RMSE (ms): {_fmt(d.get('rmse_ms'), '.6g')}  "
                f"n: {d.get('n', 'N/A')}  — run 06_fit_sort.py for train/test metrics"
            )
        else:
            print(
                f"  train RMSE (ms): {_fmt(d.get('train_rmse_ms'), '.6g')}  "
                f"R²: {_fmt(d.get('train_r2'), '.6f')}"
            )
            print(
                f"  test  RMSE (ms): {_fmt(d.get('test_rmse_ms'), '.6g')}  "
                f"MAE: {_fmt(d.get('test_mae_ms'), '.6g')}  "
                f"R²: {_fmt(d.get('test_r2'), '.6f')}  "
                f"MAPE: {_fmt(d.get('test_mape_pct'), '.4f')}%"
            )
        print()


if __name__ == "__main__":
    main()
