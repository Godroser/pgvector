#!/usr/bin/env python3
"""Linear fit for partition-local HNSW Index Scan (160 train / 40 test)."""

from __future__ import annotations

import argparse
import json
import os
import sys

_COST_FIT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _COST_FIT_ROOT not in sys.path:
    sys.path.insert(0, _COST_FIT_ROOT)

from fit_common import fit_train_test_eval, save_json  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--data",
        default=os.path.join(_COST_FIT_ROOT, "data", "hnsw_partition_scan_samples.jsonl"),
    )
    ap.add_argument(
        "--out",
        default=os.path.join(_COST_FIT_ROOT, "models", "hnsw_partition_scan_coef.json"),
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
        raise SystemExit(f"Need >= {need} samples, got {len(rows)}.")

    names = ["relpages", "reltuples", "actual_rows", "plan_width", "limit_k", "ef_search"]

    def xfn(r):
        return [
            r["relpages"],
            r["reltuples"],
            r["actual_rows"],
            r["plan_width"],
            r["limit_k"],
            r["ef_search"],
        ]

    ev = fit_train_test_eval(rows, names, xfn, train_n=args.train_n, test_n=args.test_n, seed=args.seed)
    payload = {
        "operator": "Index Scan (hnsw, partition)",
        "features": names,
        "total_samples": len(rows),
        **ev,
    }
    pred_path = os.path.join(os.path.dirname(args.out), "hnsw_partition_scan_test_predictions.jsonl")
    with open(pred_path, "w", encoding="utf-8") as pf:
        for item in ev["test_predictions"]:
            pf.write(json.dumps(item, ensure_ascii=False) + "\n")
    payload_save = {k: v for k, v in payload.items() if k != "test_predictions"}
    save_json(args.out, payload_save)
    print(json.dumps(payload_save, indent=2))
    print("wrote", args.out, pred_path)


if __name__ == "__main__":
    main()
