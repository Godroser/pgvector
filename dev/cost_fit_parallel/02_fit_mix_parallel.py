#!/usr/bin/env python3
"""
Linear fit: parallel_exclusive_ms ~ degree, focus solo time, peer solo sum, per-family
counts and per-family solo time sums (mixed concurrent batches).

Train/test split is by **batch_id** so rows from the same concurrent batch do not straddle
train and test.

Uses dev/cost_fit/fit_common.lstsq_fit — does not modify cost_fit.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
from typing import Any, Callable, Dict, List, Sequence, Set, Tuple

from mix_import_paths import ensure_paths

ensure_paths()

_COST_FIT = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "cost_fit"))
if _COST_FIT not in sys.path:
    sys.path.insert(0, _COST_FIT)

from fit_common import lstsq_fit, regression_metrics, save_json  # noqa: E402

from mix_features import feature_names  # noqa: E402


def extract_x(row: dict) -> List[float]:
    names = feature_names()
    out: List[float] = []
    for nm in names:
        if nm not in row:
            raise KeyError(f"missing feature {nm!r} in row")
        out.append(float(row[nm]))
    return out


def split_batch_ids(
    batch_ids: Sequence[str],
    train_fraction: float,
    seed: int,
) -> Tuple[Set[str], Set[str]]:
    ids = list(dict.fromkeys(batch_ids))
    rng = random.Random(seed)
    rng.shuffle(ids)
    if len(ids) < 2:
        raise SystemExit("Need at least 2 distinct batch_id values; collect more batches.")
    n_train = max(1, min(len(ids) - 1, int(round(len(ids) * train_fraction))))
    train = set(ids[:n_train])
    test = set(ids[n_train:])
    return train, test


def main() -> None:
    ap = argparse.ArgumentParser(description="Fit mixed concurrent model (batch-level split).")
    ap.add_argument(
        "--data",
        default=os.path.join(os.path.dirname(__file__), "data", "mix_parallel_samples.jsonl"),
    )
    ap.add_argument(
        "--out",
        default=os.path.join(os.path.dirname(__file__), "models", "mix_parallel_coef.json"),
    )
    ap.add_argument("--train-fraction", type=float, default=0.8, help="Fraction of batch_ids in train")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    rows: List[dict] = []
    with open(args.data, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    if len(rows) < 10:
        raise SystemExit(f"Need more rows (got {len(rows)}). Run 01_collect_mix_parallel.py.")

    batch_ids = [str(r["batch_id"]) for r in rows]
    train_b, test_b = split_batch_ids(batch_ids, args.train_fraction, args.seed)
    train_rows = [r for r in rows if str(r["batch_id"]) in train_b]
    test_rows = [r for r in rows if str(r["batch_id"]) in test_b]
    if not test_rows:
        raise SystemExit("No test batches; use more batches or lower --train-fraction.")

    names = feature_names()
    X_tr = [extract_x(r) for r in train_rows]
    y_tr = [float(r["parallel_exclusive_ms"]) for r in train_rows]
    X_te = [extract_x(r) for r in test_rows]
    y_te = [float(r["parallel_exclusive_ms"]) for r in test_rows]

    reg = lstsq_fit(names, X_tr, y_tr)
    pred_tr = [reg.predict(x) for x in X_tr]
    pred_te = [reg.predict(x) for x in X_te]
    train_m = regression_metrics(y_tr, pred_tr)
    test_m = regression_metrics(y_te, pred_te)

    test_pred_rows = []
    for r, yt, yp in zip(test_rows, y_te, pred_te):
        test_pred_rows.append(
            {
                "batch_id": r.get("batch_id", ""),
                "focus_tag": r.get("focus_tag", ""),
                "focus_operator": r.get("focus_operator", ""),
                "actual_ms": yt,
                "predicted_ms": yp,
                "abs_error_ms": abs(yp - yt),
            }
        )

    payload: Dict[str, Any] = {
        "model": "mixed concurrent: parallel_exclusive_ms ~ linear(features)",
        "features": names,
        "feature_notes": {
            "degree": "Concurrent session count (batch size)",
            "focus_solo_ms": "Primary operator solo exclusive time for the focus query",
            "sum_solo_peers_ms": "Sum of solo times of other queries in the same batch",
            "n_*": "Count of queries per operator family in the batch",
            "solo_sum_*_ms": "Sum of solo times for queries in each family (same batch)",
        },
        "train_batch_ids": sorted(train_b),
        "test_batch_ids": sorted(test_b),
        "train_rows": len(train_rows),
        "test_rows": len(test_rows),
        "intercept_ms": reg.coef[0],
        "coef": dict(zip(names, reg.coef[1:])),
        "train_rmse_ms": train_m["rmse"],
        "train_mae_ms": train_m["mae"],
        "train_r2": train_m["r2"],
        "test_rmse_ms": test_m["rmse"],
        "test_mae_ms": test_m["mae"],
        "test_r2": test_m["r2"],
        "test_mape_pct": test_m["mape_pct"],
    }

    pred_path = os.path.join(os.path.dirname(args.out) or ".", "mix_parallel_test_predictions.jsonl")
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    os.makedirs(os.path.dirname(pred_path) or ".", exist_ok=True)
    with open(pred_path, "w", encoding="utf-8") as pf:
        for item in test_pred_rows:
            pf.write(json.dumps(item, ensure_ascii=False) + "\n")

    save_json(args.out, payload)
    print(json.dumps(payload, indent=2))
    print("wrote", args.out, pred_path)


if __name__ == "__main__":
    main()
