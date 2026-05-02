#!/usr/bin/env python3
"""Ensure dev/cost_fit and dev/cost_fit_multi_parallel are on sys.path (read-only imports)."""

from __future__ import annotations

import os
import sys

_ROOT = os.path.dirname(os.path.abspath(__file__))

# Historical names in different checkouts / branches
_PARALLEL_PKG_CANDIDATES = (
    "cost_fit_multi_parallel",
    "cost_fit_same_parallel",
)


def ensure_paths() -> None:
    cost_fit = os.path.normpath(os.path.join(_ROOT, "..", "cost_fit"))
    if os.path.isdir(cost_fit) and cost_fit not in sys.path:
        sys.path.insert(0, cost_fit)

    for name in _PARALLEL_PKG_CANDIDATES:
        p = os.path.normpath(os.path.join(_ROOT, "..", name))
        if os.path.isfile(os.path.join(p, "operator_parallel.py")):
            if p not in sys.path:
                sys.path.insert(0, p)
            break
