#!/usr/bin/env python3
"""Print metrics from models/mix_parallel_coef.json."""

from __future__ import annotations

import json
import os


def main() -> None:
    path = os.path.join(os.path.dirname(__file__), "models", "mix_parallel_coef.json")
    if not os.path.isfile(path):
        print(path, "missing — run 02_fit_mix_parallel.py")
        return
    with open(path, encoding="utf-8") as f:
        d = json.load(f)
    print("=== mix_parallel ===")
    print("  train rows:", d.get("train_rows"), " test rows:", d.get("test_rows"))
    print("  train RMSE (ms):", d.get("train_rmse_ms"), " R²:", d.get("train_r2"))
    print("  test  RMSE (ms):", d.get("test_rmse_ms"), " MAE:", d.get("test_mae_ms"), " R²:", d.get("test_r2"))


if __name__ == "__main__":
    main()
