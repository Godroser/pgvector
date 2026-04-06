#!/usr/bin/env python3
"""
Linear fit: Seq Scan exclusive_ms ~ features. Train 40 / test 10 (default), total >= 50 rows required.
"""

from __future__ import annotations

import argparse
import json
import os

from fit_common import fit_train_test_eval, save_json


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default=os.path.join(os.path.dirname(__file__), "data", "scan_samples.jsonl"))
    ap.add_argument("--out", default=os.path.join(os.path.dirname(__file__), "models", "scan_coef.json"))
    ap.add_argument("--train-n", type=int, default=40)
    ap.add_argument("--test-n", type=int, default=10)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    rows = []
    with open(args.data, encoding="utf-8") as f:
        for line in f:
            if line.strip():
                rows.append(json.loads(line))
    min_need = args.train_n + args.test_n
    if len(rows) < min_need:
        raise SystemExit(f"Need >= {min_need} samples, got {len(rows)}. Run 01_collect_scan.py.")

    names = ["relpages", "reltuples", "actual_rows", "plan_width"]

    def xfn(r):
        return [r["relpages"], r["reltuples"], r["actual_rows"], r["plan_width"]]

    ev = fit_train_test_eval(
        rows,
        names,
        xfn,
        train_n=args.train_n,
        test_n=args.test_n,
        seed=args.seed,
    )
    payload = {
        "operator": "Seq Scan",
        "features": names,
        "total_samples": len(rows),
        **ev,
    }
    pred_path = os.path.join(os.path.dirname(args.out), "scan_test_predictions.jsonl")
    os.makedirs(os.path.dirname(pred_path), exist_ok=True)
    with open(pred_path, "w", encoding="utf-8") as pf:
        for item in ev["test_predictions"]:
            pf.write(json.dumps(item, ensure_ascii=False) + "\n")
    payload_save = {k: v for k, v in payload.items() if k != "test_predictions"}
    save_json(args.out, payload_save)
    print(json.dumps(payload_save, indent=2))
    print("wrote", args.out, "and", pred_path)


if __name__ == "__main__":
    main()
