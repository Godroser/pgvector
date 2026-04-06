#!/usr/bin/env python3
"""
Shared helpers: psql EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON), plan walk,
exclusive time, pg_class stats.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple


def pg_env() -> Dict[str, str]:
    e = os.environ.copy()
    e.setdefault("PGHOST", "127.0.0.1")
    e.setdefault("PGPORT", "5432")
    e.setdefault("PGUSER", "dzh")
    e.setdefault("PGDATABASE", "tpch10")
    return e


def resolve_psql() -> str:
    """
    Path to psql: $PSQL if executable, else PATH, else common install locations.
    (Calling scripts outside run_all.sh often lack PATH to psql.)
    """
    override = os.environ.get("PSQL")
    if override:
        p = os.path.expanduser(override)
        if os.path.isfile(p) and os.access(p, os.X_OK):
            return p
    which = shutil.which("psql")
    if which:
        return which
    for candidate in (
        "/data/dzh/postgresql/bin/psql",
        "/usr/local/pgsql/bin/psql",
        "/usr/bin/psql",
    ):
        if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
            return candidate
    extra = ""
    if override:
        extra = f" PSQL={override!r} is missing or not executable."
    raise RuntimeError(
        "Cannot find psql binary."
        + extra
        + " Set PSQL to the full path, or install PostgreSQL client tools and ensure psql is on PATH."
    )


def psql_sql(sql: str, tuples_only: bool = True) -> str:
    env = pg_env()
    args = [
        resolve_psql(),
        "-X",
        "-v",
        "ON_ERROR_STOP=1",
        "-d",
        env.get("PGDATABASE", "tpch10"),
        "-h",
        env.get("PGHOST", "127.0.0.1"),
        "-p",
        env.get("PGPORT", "5432"),
        "-U",
        env.get("PGUSER", "dzh"),
    ]
    if tuples_only:
        args.extend(["-t", "-A"])
    r = subprocess.run(
        args,
        input=sql.encode(),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
        check=False,
    )
    if r.returncode != 0:
        sys.stderr.write(r.stderr.decode())
        raise RuntimeError(f"psql failed ({r.returncode})")
    return r.stdout.decode()


def parse_explain_json_from_psql(out: str) -> Dict[str, Any]:
    """
    EXPLAIN FORMAT JSON is often pretty-printed across multiple lines. Parsing
    line-by-line breaks when the first line is only '[' (json.loads then fails
    with 'Expecting value: line 1 column 2').
    """
    text = out.lstrip("\ufeff").strip()
    start = text.find("[")
    if start < 0:
        raise ValueError("No '[' JSON array in psql output:\n" + text[:4000])
    dec = json.JSONDecoder()
    try:
        obj, _end = dec.raw_decode(text[start:])
    except json.JSONDecodeError as e:
        raise ValueError(
            "Invalid EXPLAIN JSON after '[' (truncated stdout or noise?):\n"
            + text[start : start + 500]
            + "\n..."
        ) from e
    if not isinstance(obj, list) or not obj:
        raise ValueError("EXPLAIN JSON must be a non-empty array")
    return obj[0]


def explain_analyze_json(sql: str, tx_prefix: str = "") -> Dict[str, Any]:
    """Run EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON). Optional tx_prefix inside same xact (SET LOCAL ...)."""
    if tx_prefix:
        wrapped = (
            f"BEGIN;\n{tx_prefix}\nEXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)\n{sql};\nCOMMIT;\n"
        )
    else:
        wrapped = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)\n{sql}\n"
    # -t -A: tuples only; JSON may still span many lines
    out = psql_sql(wrapped, tuples_only=True)
    return parse_explain_json_from_psql(out)


def child_total_time(plan: Dict[str, Any]) -> float:
    plans = plan.get("Plans") or []
    return sum(float(p.get("Actual Total Time") or 0) for p in plans)


def exclusive_total_time(plan: Dict[str, Any]) -> float:
    """Actual Total Time is inclusive of children."""
    inc = float(plan.get("Actual Total Time") or 0)
    return max(0.0, inc - child_total_time(plan))


def walk_plans(plan: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
    yield plan
    for c in plan.get("Plans") or []:
        yield from walk_plans(c)


def load_table_stats() -> Dict[str, Tuple[float, float]]:
    """relname lower -> (reltuples, relpages)."""
    sql = """
SELECT relname, COALESCE(reltuples, 0)::float, COALESCE(relpages, 0)::float
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'public' AND c.relkind = 'r';
"""
    out = psql_sql(sql.strip())
    d: Dict[str, Tuple[float, float]] = {}
    for line in out.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split("|")
        if len(parts) != 3:
            continue
        name, tup, pg = parts[0].lower(), float(parts[1]), float(parts[2])
        d[name] = (tup, pg)
    return d


def relation_name(plan: Dict[str, Any]) -> Optional[str]:
    rel = plan.get("Relation Name") or plan.get("Alias")
    if rel:
        return str(rel).lower()
    return None


def save_json(path: str, obj: Any) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, sort_keys=True)


def load_json(path: str) -> Any:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


@dataclass
class RegResult:
    names: List[str]
    coef: List[float]
    rmse: float

    def predict(self, x: List[float]) -> float:
        return self.coef[0] + sum(self.coef[i + 1] * x[i] for i in range(len(x)))


def lstsq_fit(feature_names: List[str], X: List[List[float]], y: List[float]) -> RegResult:
    try:
        import numpy as np
    except ImportError:
        raise SystemExit("Need numpy: pip install numpy")
    m = len(feature_names)
    if not X:
        raise ValueError("empty X")
    n = len(X)
    A = np.ones((n, m + 1), dtype=float)
    for i in range(n):
        for j in range(m):
            A[i, j + 1] = X[i][j]
    b = np.array(y, dtype=float)
    coef, *_ = np.linalg.lstsq(A, b, rcond=None)
    pred = A @ coef
    rmse = float(np.sqrt(np.mean((pred - b) ** 2)))
    return RegResult(names=feature_names, coef=list(map(float, coef)), rmse=rmse)


def train_test_split_rows(
    rows: List[dict], train_n: int, test_n: int, seed: int = 42
) -> Tuple[List[dict], List[dict]]:
    import random

    need = train_n + test_n
    if len(rows) < need:
        raise ValueError(f"need at least {need} samples, got {len(rows)}")
    rng = random.Random(seed)
    idx = list(range(len(rows)))
    rng.shuffle(idx)
    train = [rows[i] for i in idx[:train_n]]
    test = [rows[i] for i in idx[train_n : train_n + test_n]]
    return train, test


def regression_metrics(y_true: List[float], y_pred: List[float]) -> Dict[str, float]:
    import numpy as np

    y = np.asarray(y_true, dtype=float)
    p = np.asarray(y_pred, dtype=float)
    rmse = float(np.sqrt(np.mean((p - y) ** 2)))
    mae = float(np.mean(np.abs(p - y)))
    ss_res = float(np.sum((y - p) ** 2))
    ss_tot = float(np.sum((y - np.mean(y)) ** 2))
    r2 = float(1.0 - ss_res / ss_tot) if ss_tot > 1e-18 else 0.0
    mask = np.abs(y) > 1e-9
    mape = float(np.mean(np.abs((y[mask] - p[mask]) / y[mask])) * 100.0) if np.any(mask) else 0.0
    return {"rmse": rmse, "mae": mae, "r2": r2, "mape_pct": mape, "n": float(len(y))}


def fit_train_test_eval(
    rows: List[dict],
    feature_names: List[str],
    extract_x: Callable[[dict], List[float]],
    y_key: str = "exclusive_ms",
    train_n: int = 40,
    test_n: int = 10,
    seed: int = 42,
) -> Dict[str, Any]:
    train, test = train_test_split_rows(rows, train_n, test_n, seed)
    X_tr = [extract_x(r) for r in train]
    y_tr = [float(r[y_key]) for r in train]
    X_te = [extract_x(r) for r in test]
    y_te = [float(r[y_key]) for r in test]
    reg = lstsq_fit(feature_names, X_tr, y_tr)
    pred_tr = [reg.predict(x) for x in X_tr]
    pred_te = [reg.predict(x) for x in X_te]
    train_m = regression_metrics(y_tr, pred_tr)
    test_m = regression_metrics(y_te, pred_te)
    test_rows = []
    for r, yt, yp in zip(test, y_te, pred_te):
        test_rows.append(
            {
                "tag": r.get("tag", ""),
                "actual_ms": yt,
                "predicted_ms": yp,
                "abs_error_ms": abs(yp - yt),
            }
        )
    return {
        "intercept_ms": reg.coef[0],
        "coef": dict(zip(feature_names, reg.coef[1:])),
        "train_rmse_ms": train_m["rmse"],
        "train_mae_ms": train_m["mae"],
        "train_r2": train_m["r2"],
        "test_rmse_ms": test_m["rmse"],
        "test_mae_ms": test_m["mae"],
        "test_r2": test_m["r2"],
        "test_mape_pct": test_m["mape_pct"],
        "train_n": len(train),
        "test_n": len(test),
        "test_predictions": test_rows,
    }
