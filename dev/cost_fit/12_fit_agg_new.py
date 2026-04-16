#!/usr/bin/env python3
"""
Linear fit: Aggregate exclusive_ms ~ cost-ascii.md §6-style features.

See 11_collect_agg_new.py for N, g, G proxies.

Defaults: data/agg_samples_new.jsonl, models/agg_coef_new.json,
         models/agg_test_predictions_new.jsonl
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
        default=os.path.join(os.path.dirname(__file__), "data", "agg_samples_new.jsonl"),
    )
    ap.add_argument(
        "--out",
        default=os.path.join(os.path.dirname(__file__), "models", "agg_coef_new.json"),
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
        raise SystemExit(f"Need >= {need} samples, got {len(rows)}. Run 11_collect_agg_new.py.")

    names = [
        "N_child_plan",
        "num_group_keys",
        "G_plan",
        "N_times_g_plan",
        "G_times_width_proxy",
    ]

    def xfn(r):
        return [
            r["N_child_plan"],
            float(r["num_group_keys"]),
            r["G_plan"],
            r["N_times_g_plan"],
            r["G_times_width_proxy"],
        ]

    ev = fit_train_test_eval(rows, names, xfn, train_n=args.train_n, test_n=args.test_n, seed=args.seed)
    payload = {
        "operator": "Aggregate",
        "cost_ascii_section": "§6 Aggregate",
        "features": names,
        "feature_notes": {
            "N_child_plan": "doc N; child Plan Rows",
            "num_group_keys": "doc g from Group Key length",
            "G_plan": "doc G; agg node Plan Rows (groups estimate)",
            "N_times_g_plan": "g * C_op * N scale (HASHED/SORTED style)",
            "G_times_width_proxy": "C_tuple*G + output width proxy",
            "intercept": "trans/final AggClauseCosts + spill omitted",
        },
        "total_samples": len(rows),
        **ev,
    }
    pred_path = os.path.join(os.path.dirname(args.out), "agg_test_predictions_new.jsonl")
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
