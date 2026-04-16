#!/usr/bin/env python3
"""
Linear fit: Hash Join exclusive_ms ~ cost-ascii.md §4-style features.

See 07_collect_hashjoin_new.py for R_O/R_I/k/J/spill substitutes.

Defaults: data/hashjoin_samples_new.jsonl, models/hashjoin_coef_new.json,
models/hashjoin_test_predictions_new.jsonl
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
        default=os.path.join(os.path.dirname(__file__), "data", "hashjoin_samples_new.jsonl"),
    )
    ap.add_argument(
        "--out",
        default=os.path.join(os.path.dirname(__file__), "models", "hashjoin_coef_new.json"),
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
        raise SystemExit(f"Need >= {need} samples, got {len(rows)}. Run 07_collect_hashjoin_new.py.")

    names = [
        "R_outer_times_k",
        "R_inner_plan",
        "J_plan",
        "proj_proxy_join_bytes",
        "hash_spill_proxy",
        "hash_spill_rows_proxy",
    ]

    def xfn(r):
        return [
            r["R_outer_times_k"],
            r["R_inner_plan"],
            r["J_plan"],
            r["proj_proxy_join_bytes"],
            r["hash_spill_proxy"],
            r["hash_spill_rows_proxy"],
        ]

    ev = fit_train_test_eval(rows, names, xfn, train_n=args.train_n, test_n=args.test_n, seed=args.seed)
    payload = {
        "operator": "Hash Join",
        "cost_ascii_section": "§4 Hash Join",
        "features": names,
        "feature_notes": {
            "R_outer_times_k": "R_O * k term scale (plan-time)",
            "R_inner_plan": "build side size R_I (plan rows inner)",
            "J_plan": "join output J (Plan Rows)",
            "proj_proxy_join_bytes": "T_pt * R_out proxy",
            "hash_spill_proxy": "1 if Hash Batches > 1",
            "hash_spill_rows_proxy": "spill * (outer_plan+inner_plan) for multi-batch I/O scale",
            "intercept": "O/I child path constants + f_bucket / inner_unique omitted",
        },
        "total_samples": len(rows),
        **ev,
    }
    pred_path = os.path.join(os.path.dirname(args.out), "hashjoin_test_predictions_new.jsonl")
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
