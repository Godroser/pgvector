#!/usr/bin/env python3
"""
Linear fit: Sort exclusive_ms ~ cost-ascii.md §5 tuplesort-style features.

See 05_collect_sort_new.py for N_sort_logn / spill / width proxies.

Defaults: data/sort_samples_new.jsonl, models/sort_coef_new.json,
models/sort_test_predictions_new.jsonl
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
        default=os.path.join(os.path.dirname(__file__), "data", "sort_samples_new.jsonl"),
    )
    ap.add_argument(
        "--out",
        default=os.path.join(os.path.dirname(__file__), "models", "sort_coef_new.json"),
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
        raise SystemExit(f"Need >= {need} samples, got {len(rows)}. Run 05_collect_sort_new.py.")

    names = [
        "N_sort_logn",
        "N_sort_plan",
        "N_sort_spill_proxy_rows",
        "sort_tuple_bytes_proxy",
    ]

    def xfn(r):
        return [
            r["N_sort_logn"],
            r["N_sort_plan"],
            r["N_sort_spill_proxy_rows"],
            r["sort_tuple_bytes_proxy"],
        ]

    ev = fit_train_test_eval(rows, names, xfn, train_n=args.train_n, test_n=args.test_n, seed=args.seed)
    payload = {
        "operator": "Sort",
        "cost_ascii_section": "§5 Sort / cost_tuplesort",
        "features": names,
        "feature_notes": {
            "N_sort_logn": "C_cmp * N * log2(N) scale; N from Plan Rows",
            "N_sort_plan": "C_op * N run term scale",
            "N_sort_spill_proxy_rows": "external sort I/O proxy (N * external flag)",
            "sort_tuple_bytes_proxy": "width * rows for memory / byte workload",
            "intercept": "input path + omitted sort constants",
        },
        "total_samples": len(rows),
        **ev,
    }
    pred_path = os.path.join(os.path.dirname(args.out), "sort_test_predictions_new.jsonl")
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
