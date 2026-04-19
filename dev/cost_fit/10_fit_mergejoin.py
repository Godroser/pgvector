#!/usr/bin/env python3
"""Merge Join fit: train 160 / test 40."""

from __future__ import annotations

import argparse
import json
import os

from fit_common import fit_train_test_eval, save_json


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default=os.path.join(os.path.dirname(__file__), "data", "mergejoin_samples.jsonl"))
    ap.add_argument("--out", default=os.path.join(os.path.dirname(__file__), "models", "mergejoin_coef.json"))
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

    names = ["outer_actual_rows", "inner_actual_rows", "actual_rows", "plan_width"]

    def xfn(r):
        return [r["outer_actual_rows"], r["inner_actual_rows"], r["actual_rows"], r["plan_width"]]

    ev = fit_train_test_eval(rows, names, xfn, train_n=args.train_n, test_n=args.test_n, seed=args.seed)
    payload = {"operator": "Merge Join", "features": names, "total_samples": len(rows), **ev}
    ppath = os.path.join(os.path.dirname(args.out), "mergejoin_test_predictions.jsonl")
    with open(ppath, "w", encoding="utf-8") as pf:
        for item in ev["test_predictions"]:
            pf.write(json.dumps(item, ensure_ascii=False) + "\n")
    payload_save = {k: v for k, v in payload.items() if k != "test_predictions"}
    save_json(args.out, payload_save)
    print(json.dumps(payload_save, indent=2))
    print("wrote", args.out, ppath)


if __name__ == "__main__":
    main()
