#!/usr/bin/env python3
"""
Linear fit: Index Scan exclusive_ms ~ cost-ascii.md §3-style features.

See 03_collect_index_scan_new.py module docstring for symbol ↔ column mapping.

Intercept absorbs indexStartup / qual startup / correlation effects not modeled.

Defaults: data/index_scan_samples_new.jsonl, models/index_scan_coef_new.json,
models/index_scan_test_predictions_new.jsonl
"""

from __future__ import annotations

import argparse
import json
import os

from fit_common import fit_train_test_eval, save_json


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--data",
        default=os.path.join(os.path.dirname(__file__), "data", "index_scan_samples_new.jsonl"),
    )
    ap.add_argument(
        "--out",
        default=os.path.join(os.path.dirname(__file__), "models", "index_scan_coef_new.json"),
    )
    ap.add_argument("--train-n", type=int, default=40)
    ap.add_argument("--test-n", type=int, default=10)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    rows = []
    with open(args.data, encoding="utf-8") as f:
        for line in f:
            if line.strip():
                rows.append(json.loads(line))
    need = args.train_n + args.test_n
    if len(rows) < need:
        raise SystemExit(f"Need >= {need} samples, got {len(rows)}. Run 03_collect_index_scan_new.py.")

    names = [
        "P_idx_access_est",
        "N_heap_tuples",
        "N_fetch_plan",
        "proj_proxy_plan_row_bytes",
        "N_fetch_times_qpqual_proxy",
    ]

    def xfn(r):
        return [
            r["P_idx_access_est"],
            r["N_heap_tuples"],
            r["N_fetch_plan"],
            r["proj_proxy_plan_row_bytes"],
            r["N_fetch_times_qpqual_proxy"],
        ]

    ev = fit_train_test_eval(rows, names, xfn, train_n=args.train_n, test_n=args.test_n, seed=args.seed)
    payload = {
        "operator": "Index Scan",
        "cost_ascii_section": "§3 Index Scan / §3.1 genericcostestimate",
        "features": names,
        "feature_notes": {
            "P_idx_access_est": "substitute P_idx from index stats + Plan Rows (§3.1)",
            "N_heap_tuples": "N_heap for correlation context",
            "N_fetch_plan": "substitute N_fetch (~ plan-time row/fetch estimate)",
            "proj_proxy_plan_row_bytes": "T_pt * R_out via Plan Rows * Plan Width",
            "N_fetch_times_qpqual_proxy": "(C_tuple+Qqp)*N scale; qp from Index Cond/Filter/Recheck AND counts",
            "intercept": "index startup + Mackert + omitted terms",
        },
        "total_samples": len(rows),
        **ev,
    }
    pred_path = os.path.join(os.path.dirname(args.out), "index_scan_test_predictions_new.jsonl")
    os.makedirs(os.path.dirname(pred_path), exist_ok=True)
    with open(pred_path, "w", encoding="utf-8") as pf:
        for item in ev["test_predictions"]:
            pf.write(json.dumps(item, ensure_ascii=False) + "\n")
    payload_save = {k: v for k, v in payload.items() if k != "test_predictions"}
    save_json(args.out, payload_save)
    print(json.dumps(payload_save, indent=2))
    print("wrote", args.out, pred_path)


if __name__ == "__main__":
    main()
