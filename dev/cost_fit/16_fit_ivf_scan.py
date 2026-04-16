#!/usr/bin/env python3
"""IVFFlat vector Index Scan fit: grouped per index, log target, interaction terms."""

from __future__ import annotations

import argparse
import json
import math
import os
from collections import defaultdict

from fit_common import lstsq_fit, regression_metrics, save_json, train_test_split_rows


def _safe_log1p(x: float) -> float:
    return math.log1p(max(0.0, float(x)))


def _feature_map(r: dict) -> dict[str, float]:
    probes_over_lists = float(r.get("probes_over_lists") or 0.0)
    log_planner_startup_cost = _safe_log1p(r.get("planner_startup_cost") or 0.0)
    log_ivf_startup_cost_est = _safe_log1p(r.get("ivf_startup_cost_est") or 0.0)
    log_estimated_candidates = _safe_log1p(r.get("estimated_candidates") or 0.0)
    log_estimated_startup_pages = _safe_log1p(r.get("estimated_startup_pages") or 0.0)
    log_limit_k = _safe_log1p(r.get("limit_k") or 0.0)
    log_query_l2_norm = _safe_log1p(r.get("query_l2_norm") or 0.0)
    query_ref_dist_min = float(r.get("query_ref_dist_min") or 0.0)
    query_ref_dist_avg = float(r.get("query_ref_dist_avg") or 0.0)
    query_ref_dist_std = float(r.get("query_ref_dist_std") or 0.0)
    anchor_offset_log = _safe_log1p(r.get("anchor_offset") or 0.0)
    plan_width = float(r.get("plan_width") or 0.0)
    return {
        "log_planner_startup_cost": log_planner_startup_cost,
        "log_ivf_startup_cost_est": log_ivf_startup_cost_est,
        "log_estimated_candidates": log_estimated_candidates,
        "log_estimated_startup_pages": log_estimated_startup_pages,
        "log_limit_k": log_limit_k,
        "probes_over_lists": probes_over_lists,
        "log_query_l2_norm": log_query_l2_norm,
        "query_ref_dist_min": query_ref_dist_min,
        "query_ref_dist_avg": query_ref_dist_avg,
        "query_ref_dist_std": query_ref_dist_std,
        "anchor_offset_log": anchor_offset_log,
        "plan_width": plan_width,
        "probes_x_log_candidates": probes_over_lists * log_estimated_candidates,
        "probes_x_log_limit": probes_over_lists * log_limit_k,
        "query_min_x_probes": query_ref_dist_min * probes_over_lists,
        "query_std_x_log_limit": query_ref_dist_std * log_limit_k,
        "log_candidates_x_log_limit": log_estimated_candidates * log_limit_k,
        "query_avg_x_probes": query_ref_dist_avg * probes_over_lists,
    }


FEATURE_NAMES = list(_feature_map({}).keys())


def _predict_ms(reg, x: list[float]) -> float:
    return max(0.0, math.expm1(reg.predict(x)))


def _fit_group(rows: list[dict], feature_names: list[str], train_ratio: float, seed: int) -> dict:
    if len(rows) < 4:
        raise ValueError(f"Need >= 4 samples per IVFFlat index, got {len(rows)}.")
    train_n = int(round(len(rows) * train_ratio))
    train_n = min(len(rows) - 1, max(2, train_n))
    test_n = len(rows) - train_n
    train_rows, test_rows = train_test_split_rows(rows, train_n=train_n, test_n=test_n, seed=seed)
    X_tr = [[_feature_map(r)[n] for n in feature_names] for r in train_rows]
    y_tr_raw = [float(r["exclusive_ms"]) for r in train_rows]
    y_tr = [_safe_log1p(v) for v in y_tr_raw]
    X_te = [[_feature_map(r)[n] for n in feature_names] for r in test_rows]
    y_te_raw = [float(r["exclusive_ms"]) for r in test_rows]
    reg = lstsq_fit(feature_names, X_tr, y_tr)
    pred_tr = [_predict_ms(reg, x) for x in X_tr]
    pred_te = [_predict_ms(reg, x) for x in X_te]
    train_m = regression_metrics(y_tr_raw, pred_tr)
    test_m = regression_metrics(y_te_raw, pred_te)
    test_predictions = []
    for row, yt, yp in zip(test_rows, y_te_raw, pred_te):
        test_predictions.append(
            {
                "tag": row.get("tag", ""),
                "index_name": row.get("index_name", ""),
                "actual_ms": yt,
                "predicted_ms": yp,
                "abs_error_ms": abs(yp - yt),
            }
        )
    return {
        "reg": reg,
        "train_rows": train_rows,
        "test_rows": test_rows,
        "train_pred": pred_tr,
        "test_pred": pred_te,
        "train_actual": y_tr_raw,
        "test_actual": y_te_raw,
        "metrics": {
            "train_rmse_ms": train_m["rmse"],
            "train_mae_ms": train_m["mae"],
            "train_r2": train_m["r2"],
            "test_rmse_ms": test_m["rmse"],
            "test_mae_ms": test_m["mae"],
            "test_r2": test_m["r2"],
            "test_mape_pct": test_m["mape_pct"],
            "train_n": len(train_rows),
            "test_n": len(test_rows),
        },
        "test_predictions": test_predictions,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default=os.path.join(os.path.dirname(__file__), "data", "ivf_scan_samples.jsonl"))
    ap.add_argument("--out", default=os.path.join(os.path.dirname(__file__), "models", "ivf_scan_coef.json"))
    ap.add_argument("--train-n", type=int, default=160)
    ap.add_argument("--test-n", type=int, default=40)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    rows = []
    with open(args.data, encoding="utf-8") as f:
        for line in f:
            if line.strip():
                rows.append(json.loads(line))
    need = args.train_n + args.test_n
    if len(rows) < need:
        raise SystemExit(f"Need >= {need} samples, got {len(rows)}.")

    grouped: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get("index_name") or "").lower()].append(row)
    if not grouped:
        raise SystemExit("No IVFFlat rows grouped by index_name.")

    train_ratio = float(args.train_n) / float(args.train_n + args.test_n)
    models_by_index = {}
    all_train_actual: list[float] = []
    all_train_pred: list[float] = []
    all_test_actual: list[float] = []
    all_test_pred: list[float] = []
    all_test_predictions: list[dict] = []

    for group_idx, index_name in enumerate(sorted(grouped.keys())):
        fit = _fit_group(grouped[index_name], FEATURE_NAMES, train_ratio, args.seed + group_idx)
        reg = fit["reg"]
        all_train_actual.extend(fit["train_actual"])
        all_train_pred.extend(fit["train_pred"])
        all_test_actual.extend(fit["test_actual"])
        all_test_pred.extend(fit["test_pred"])
        all_test_predictions.extend(fit["test_predictions"])
        models_by_index[index_name] = {
            "index_name": index_name,
            "target_transform": "log1p(exclusive_ms)",
            "feature_names": FEATURE_NAMES,
            "intercept_log_ms": reg.coef[0],
            "coef": dict(zip(FEATURE_NAMES, reg.coef[1:])),
            "total_samples": len(grouped[index_name]),
            **fit["metrics"],
        }

    train_m = regression_metrics(all_train_actual, all_train_pred)
    test_m = regression_metrics(all_test_actual, all_test_pred)
    payload = {
        "operator": "Index Scan (ivfflat)",
        "modeling_strategy": "separate linear model per index_name with log1p target and interaction terms",
        "target_transform": "log1p(exclusive_ms)",
        "features": FEATURE_NAMES,
        "total_samples": len(rows),
        "train_n": len(all_train_actual),
        "test_n": len(all_test_actual),
        "train_rmse_ms": train_m["rmse"],
        "train_mae_ms": train_m["mae"],
        "train_r2": train_m["r2"],
        "test_rmse_ms": test_m["rmse"],
        "test_mae_ms": test_m["mae"],
        "test_r2": test_m["r2"],
        "test_mape_pct": test_m["mape_pct"],
        "models_by_index": models_by_index,
        "test_predictions": all_test_predictions,
    }
    pred_path = os.path.join(os.path.dirname(args.out), "ivf_scan_test_predictions.jsonl")
    with open(pred_path, "w", encoding="utf-8") as pf:
        for item in all_test_predictions:
            pf.write(json.dumps(item, ensure_ascii=False) + "\n")
    payload_save = {k: v for k, v in payload.items() if k != "test_predictions"}
    save_json(args.out, payload_save)
    print(json.dumps(payload_save, indent=2))
    print("wrote", args.out, pred_path)


if __name__ == "__main__":
    main()
