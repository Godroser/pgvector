#!/usr/bin/env python3
"""
Linear fit: Seq Scan exclusive_ms ~ cost-ascii.md §2 style features.

Target (label): exclusive_ms (from EXPLAIN ANALYZE), same as 02_fit_scan.py.

Features (must match 01_collect_scan_new.py):
  P_pages                  — doc P; disk_run uses C_seq * P.
  N_heap_tuples            — doc N; cpu_run uses (C_tuple + Qs_pt) * N; Qs_pt not known,
                             partially absorbed via N_times_qual_proxy.
  proj_proxy_plan_row_bytes — substitute for T_pt * R_out (Plan Rows * Plan Width).
  N_times_qual_proxy       — substitute for extra qual CPU ~ Qs_pt * N (see collect docstring).

Qs_su + T_su (startup) are not separate regressors; the fitted intercept absorbs them plus
any constant bias.

Original 02_fit_scan.py is unchanged. Defaults: data/scan_samples_new.jsonl,
models/scan_coef_new.json, models/scan_test_predictions_new.jsonl
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
        default=os.path.join(os.path.dirname(__file__), "data", "scan_samples_new.jsonl"),
    )
    ap.add_argument(
        "--out",
        default=os.path.join(os.path.dirname(__file__), "models", "scan_coef_new.json"),
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
    min_need = args.train_n + args.test_n
    if len(rows) < min_need:
        raise SystemExit(f"Need >= {min_need} samples, got {len(rows)}. Run 01_collect_scan_new.py.")

    names = [
        "P_pages",
        "N_heap_tuples",
        "proj_proxy_plan_row_bytes",
        "N_times_qual_proxy",
    ]

    def xfn(r):
        return [
            r["P_pages"],
            r["N_heap_tuples"],
            r["proj_proxy_plan_row_bytes"],
            r["N_times_qual_proxy"],
        ]

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
        "cost_ascii_section": "§2 Seq Scan + filter + projection",
        "features": names,
        "feature_notes": {
            "P_pages": "doc P (baserel->pages); from pg_class.relpages",
            "N_heap_tuples": "doc N; pg_class.reltuples — full-table seq scan assumption",
            "proj_proxy_plan_row_bytes": "substitute for T_pt * R_out = Plan Rows * Plan Width",
            "N_times_qual_proxy": "substitute for Qs_pt * N scale; N * Filter AND-clause count",
            "intercept": "absorbs Qs_su + T_su and constant bias (not separate in EXPLAIN)",
        },
        "total_samples": len(rows),
        **ev,
    }
    pred_path = os.path.join(os.path.dirname(args.out), "scan_test_predictions_new.jsonl")
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
