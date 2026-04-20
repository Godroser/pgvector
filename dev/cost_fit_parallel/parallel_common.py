#!/usr/bin/env python3
"""
Helpers for concurrent EXPLAIN (ANALYZE) sessions: path to parent cost_fit, wall-clock
batch timing, and baseline loading from cost_fit/data/*.jsonl (read-only).
"""

from __future__ import annotations

import json
import math
import os
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Dict, List, Optional, Tuple

# Parent package: dev/cost_fit (fit_common, workloads)
_COST_FIT_DIR = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "cost_fit"))
if _COST_FIT_DIR not in sys.path:
    sys.path.insert(0, _COST_FIT_DIR)

from fit_common import explain_analyze_json  # noqa: E402


def cost_fit_dir() -> str:
    return _COST_FIT_DIR


def session_prelude_sql() -> str:
    path = os.path.join(_COST_FIT_DIR, "session_prelude.sql")
    with open(path, encoding="utf-8") as f:
        return f.read().strip()


def load_single_baselines(jsonl_path: str) -> Dict[str, float]:
    """tag -> exclusive_ms from cost_fit-style samples (must contain tag + exclusive_ms)."""
    out: Dict[str, float] = {}
    if not os.path.isfile(jsonl_path):
        return out
    with open(jsonl_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            tag = row.get("tag")
            if tag is None:
                continue
            out[str(tag)] = float(row["exclusive_ms"])
    return out


def percentile_sorted(sorted_vals: List[float], p: float) -> float:
    """Linear interpolation p in [0,100], sorted_vals non-empty."""
    if not sorted_vals:
        return float("nan")
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    p = max(0.0, min(100.0, p))
    k = (len(sorted_vals) - 1) * (p / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_vals[int(k)]
    return sorted_vals[f] + (sorted_vals[c] - sorted_vals[f]) * (k - f)


def summarize_times(ms: List[float]) -> Dict[str, float]:
    if not ms:
        return {
            "median_ms": float("nan"),
            "mean_ms": float("nan"),
            "min_ms": float("nan"),
            "max_ms": float("nan"),
            "p90_ms": float("nan"),
        }
    s = sorted(ms)
    return {
        "median_ms": float(statistics.median(s)),
        "mean_ms": float(sum(s) / len(s)),
        "min_ms": float(s[0]),
        "max_ms": float(s[-1]),
        "p90_ms": percentile_sorted(s, 90.0),
    }


def concurrent_explain_runs(
    degree: int,
    run_once: Callable[[], Any],
) -> Tuple[List[Any], float]:
    """
    Launch `degree` concurrent sessions, each calling run_once() which should run
    EXPLAIN ANALYZE and return a value (typically exclusive_ms). Returns (results, wall_ms).
    """
    if degree < 1:
        raise ValueError("degree must be >= 1")

    def job() -> Any:
        return run_once()

    t0 = time.perf_counter()
    results: List[Any] = []
    with ThreadPoolExecutor(max_workers=degree) as pool:
        futures = [pool.submit(job) for _ in range(degree)]
        for fut in as_completed(futures):
            results.append(fut.result())
    wall_ms = (time.perf_counter() - t0) * 1000.0
    return results, wall_ms


def explain_one(sql: str, tx_prefix: str) -> Dict[str, Any]:
    return explain_analyze_json(sql, tx_prefix=tx_prefix)
