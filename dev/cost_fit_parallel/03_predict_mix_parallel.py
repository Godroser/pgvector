#!/usr/bin/env python3
"""
Predict parallel_exclusive_ms for a focus query given a mixed batch description.

Input JSON (stdin or --json-file) keys:
  - operators_in_batch: list of operator family names, e.g. ["sort","hashjoin","scan"]
  - solo_ms: list of solo exclusive times aligned with operators_in_batch
  - focus_index: which query is the focus (0-based)

Alternatively pass --coef and the same fields flattened via CLI is not supported; use JSON.

Example:
  echo '{"operators_in_batch":["sort","hashjoin"],"solo_ms":[1.2,40.5],"focus_index":1}' |
    python3 03_predict_mix_parallel.py --coef models/mix_parallel_coef.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys

from mix_features import feature_row_for_focus, predict_ms


def main() -> None:
    ap = argparse.ArgumentParser(description="Predict mixed concurrent exclusive ms.")
    ap.add_argument(
        "--coef",
        default=os.path.join(os.path.dirname(__file__), "models", "mix_parallel_coef.json"),
    )
    ap.add_argument("--json-file", default="", help="Read batch JSON from file (default: stdin)")
    args = ap.parse_args()

    if args.json_file:
        with open(args.json_file, encoding="utf-8") as f:
            payload = json.load(f)
    else:
        payload = json.load(sys.stdin)

    ops = payload["operators_in_batch"]
    solos = payload["solo_ms"]
    focus_idx = int(payload["focus_index"])
    degree = int(payload.get("degree", len(ops)))
    if len(ops) != len(solos):
        raise SystemExit("operators_in_batch and solo_ms must have same length")
    if degree != len(ops):
        raise SystemExit("degree must match batch size unless extended later")

    row = feature_row_for_focus(degree, ops, solos, focus_idx)
    with open(args.coef, encoding="utf-8") as f:
        coef = json.load(f)

    yhat = predict_ms(coef, row)
    out = {
        "predicted_parallel_exclusive_ms": yhat,
        "features": row,
    }
    print(json.dumps(out, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
