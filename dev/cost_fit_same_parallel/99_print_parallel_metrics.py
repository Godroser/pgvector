#!/usr/bin/env python3
"""Print train/test metrics from models/*_parallel_coef.json (after 02_fit_parallel.py)."""

from __future__ import annotations

import json
import os
from typing import Any


def _fmt(x: Any, spec: str) -> str:
    if x is None:
        return "N/A"
    try:
        return format(float(x), spec)
    except (TypeError, ValueError):
        return str(x)


def main() -> None:
    base = os.path.join(os.path.dirname(__file__), "models")
    if not os.path.isdir(base):
        print("models/ missing — run 02_fit_parallel.py first")
        return
    names = sorted(
        n
        for n in os.listdir(base)
        if n.endswith("_parallel_coef.json")
    )
    if not names:
        print("No models/*_parallel_coef.json files found")
        return
    for name in names:
        path = os.path.join(base, name)
        with open(path, encoding="utf-8") as f:
            d = json.load(f)
        op = d.get("operator", name)
        print(f"=== {op} (parallel) ===")
        ts, tn, ten = d.get("total_samples"), d.get("train_n"), d.get("test_n")
        print(
            f"  samples: {ts if ts is not None else 'N/A'}  "
            f"train: {tn if tn is not None else 'N/A'}  "
            f"test: {ten if ten is not None else 'N/A'}"
        )
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
