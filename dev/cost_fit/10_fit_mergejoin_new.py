#!/usr/bin/env python3
"""
Linear fit: Merge Join exclusive_ms ~ cost-ascii.md §5 merge-join output / probe scale.

See 09_collect_mergejoin_new.py for features.

Defaults: data/mergejoin_samples_new.jsonl, models/mergejoin_coef_new.json,
models/mergejoin_test_predictions_new.jsonl
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
        default=os.path.join(os.path.dirname(__file__), "data", "mergejoin_samples_new.jsonl"),
    )
    ap.add_argument(
        "--out",
        default=os.path.join(os.path.dirname(__file__), "models", "mergejoin_coef_new.json"),
    )
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
        raise SystemExit(f"Need >= {need} samples, got {len(rows)}. Run 09_collect_mergejoin_new.py.")

    names = [
        "R_outer_times_k",
        "R_inner_plan",
        "J_plan",
        "proj_proxy_join_bytes",
    ]

    def xfn(r):
        return [
            r["R_outer_times_k"],
            r["R_inner_plan"],
            r["J_plan"],
            r["proj_proxy_join_bytes"],
        ]

    ev = fit_train_test_eval(rows, names, xfn, train_n=args.train_n, test_n=args.test_n, seed=args.seed)
    payload = {
        "operator": "Merge Join",
        "cost_ascii_section": "§5 Merge Join",
        "features": names,
        "feature_notes": {
            "R_outer_times_k": "outer plan rows * merge_k (Merge Cond count)",
            "R_inner_plan": "inner plan rows",
            "J_plan": "join output J (Plan Rows)",
            "proj_proxy_join_bytes": "T_pt * R_out proxy",
            "intercept": "rescanratio / mat_inner vs bare_inner + mergequals detail omitted",
        },
        "total_samples": len(rows),
        **ev,
    }
    pred_path = os.path.join(os.path.dirname(args.out), "mergejoin_test_predictions_new.jsonl")
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
