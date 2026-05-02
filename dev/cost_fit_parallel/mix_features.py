#!/usr/bin/env python3
"""Shared feature construction for mixed concurrent batches (counts + solo sums by family)."""

from __future__ import annotations

from typing import Dict, List, Sequence, Tuple

OPERATOR_KEYS: Sequence[str] = (
    "sort",
    "mergejoin",
    "hashjoin",
    "scan",
    "index_scan",
    "agg",
    "ivf_scan",
)


def counts_and_solo_sums(
    operators: Sequence[str],
    solos: Sequence[float],
) -> Tuple[Dict[str, int], Dict[str, float]]:
    if len(operators) != len(solos):
        raise ValueError("operators and solos length mismatch")
    n_T = {k: 0 for k in OPERATOR_KEYS}
    sum_T = {k: 0.0 for k in OPERATOR_KEYS}
    for op, s in zip(operators, solos):
        if op not in n_T:
            raise ValueError(f"unknown operator family {op!r}")
        n_T[op] += 1
        sum_T[op] += float(s)
    return n_T, sum_T


def feature_names() -> List[str]:
    names = [
        "degree",
        "focus_solo_ms",
        "sum_solo_peers_ms",
    ]
    for k in OPERATOR_KEYS:
        names.append(f"n_{k}")
    for k in OPERATOR_KEYS:
        names.append(f"solo_sum_{k}_ms")
    return names


def feature_row_for_focus(
    degree: int,
    operators: Sequence[str],
    solos: Sequence[float],
    focus_idx: int,
) -> Dict[str, float]:
    """Build flat feature dict matching 02_fit_mix_parallel.extract_x expectations."""
    if focus_idx < 0 or focus_idx >= len(operators):
        raise IndexError("focus_idx out of range")
    focus_solo = float(solos[focus_idx])
    total = float(sum(float(s) for s in solos))
    peer = total - focus_solo
    n_T, sum_T = counts_and_solo_sums(operators, solos)
    row: Dict[str, float] = {
        "degree": float(degree),
        "focus_solo_ms": focus_solo,
        "sum_solo_peers_ms": peer,
    }
    for k in OPERATOR_KEYS:
        row[f"n_{k}"] = float(n_T[k])
    for k in OPERATOR_KEYS:
        row[f"solo_sum_{k}_ms"] = float(sum_T[k])
    return row


def predict_ms(coef_json: dict, row: Dict[str, float]) -> float:
    """Apply saved mix_parallel_coef.json to a feature row."""
    names: List[str] = coef_json["features"]
    intercept = float(coef_json["intercept_ms"])
    c = coef_json["coef"]
    x = [float(row[nm]) for nm in names]
    b = intercept
    for nm, xv in zip(names, x):
        b += float(c[nm]) * xv
    return float(b)
