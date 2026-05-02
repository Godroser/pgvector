#!/usr/bin/env python3
"""
Sweep session work_mem for an IVFFlat ANN query on tpch10 (part / partsupp).

Prerequisites (see dev/load_table_tpch10.sql and dev/cost_fit/prepare_vector_indexes_ivf.sql):
  - Tables part / partsupp with vector columns and IVFFlat indexes.

Default connection matches cost_fit helpers: PSQL or /data/dzh/postgresql/bin/psql,
PGDATABASE=tpch10, PGHOST/PGPORT/PGUSER from environment.
"""

from __future__ import annotations

import argparse
import os
import statistics
import sys

from fit_common import explain_analyze_json, resolve_psql
from workloads import _pgvector_ann_sql_part, _pgvector_ann_sql_partsupp


def _session_prelude() -> str:
    path = os.path.join(os.path.dirname(__file__), "session_prelude.sql")
    with open(path, encoding="utf-8") as f:
        return f.read().strip() + "\n"


def _build_sql(table: str, limit: int, anchor_off: int) -> str:
    if table == "part":
        return _pgvector_ann_sql_part(limit, anchor_off)
    if table == "partsupp":
        return _pgvector_ann_sql_partsupp(limit, anchor_off)
    raise ValueError(f"table must be part or partsupp, got {table!r}")


def _execution_ms(explain_root: dict) -> float:
    et = explain_root.get("Execution Time")
    if et is not None:
        return float(et)
    plan = explain_root.get("Plan") or {}
    return float(plan.get("Actual Total Time") or 0.0)


def _run_once(tx_prefix: str, sql: str) -> float:
    root = explain_analyze_json(sql, tx_prefix=tx_prefix)
    return _execution_ms(root)


def main() -> int:
    ap = argparse.ArgumentParser(description="Measure IVFFlat ANN time vs work_mem.")
    ap.add_argument(
        "--work-mem",
        default="4MB,16MB,32MB,64MB,256MB,1GB",
        help="Comma-separated work_mem values for SET LOCAL (default: 4MB,16MB,64MB,256MB,1GB)",
    )
    ap.add_argument("--table", choices=("part", "partsupp"), default="part")
    ap.add_argument("--probes", type=int, default=10, help="ivfflat.probes (SET LOCAL)")
    ap.add_argument("--limit", type=int, default=100, help="ANN LIMIT k")
    ap.add_argument("--anchor-offset", type=int, default=131, help="OFFSET for query vector row")
    ap.add_argument("--warmup", type=int, default=1, help="Extra runs per work_mem (discarded)")
    ap.add_argument("--repeats", type=int, default=5, help="Timed runs per work_mem after warmup")
    ap.add_argument(
        "--psql",
        default="",
        help="Override psql path (else PSQL env or /data/dzh/postgresql/bin/psql)",
    )
    ap.add_argument(
        "--database",
        default=os.environ.get("PGDATABASE", "tpch10"),
        help="Database name (default: tpch10 or PGDATABASE)",
    )
    args = ap.parse_args()

    if args.psql:
        os.environ["PSQL"] = args.psql
    os.environ["PGDATABASE"] = args.database
    # Document expected binary for users who copy the command line.
    _ = resolve_psql()

    work_mems = [x.strip() for x in args.work_mem.split(",") if x.strip()]
    if not work_mems:
        print("No work_mem values given.", file=sys.stderr)
        return 2

    sql_body = _build_sql(args.table, args.limit, args.anchor_offset)
    # Strip workload comment lead if present (harmless for EXPLAIN).
    if sql_body.startswith("--"):
        sql_body = "\n".join(sql_body.splitlines()[1:]).lstrip()

    prelude = _session_prelude()
    force_seq = "SET LOCAL enable_seqscan TO off;\n"

    print(
        "work_mem\tmedian_ms\tmean_ms\tstdev_ms\tmin_ms\tmax_ms\truns\t"
        f"table={args.table} probes={args.probes} limit={args.limit} offset={args.anchor_offset}"
    )

    for wm in work_mems:
        tx = (
            f"{prelude}{force_seq}"
            f"SET LOCAL ivfflat.probes TO {args.probes};\n"
            f"SET LOCAL work_mem TO '{wm}';\n"
        )
        for _ in range(max(0, args.warmup)):
            _run_once(tx, sql_body)
        times: list[float] = []
        for _ in range(max(1, args.repeats)):
            times.append(_run_once(tx, sql_body))
        med = statistics.median(times)
        mean = statistics.mean(times)
        sd = statistics.stdev(times) if len(times) > 1 else 0.0
        print(
            f"{wm}\t{med:.4f}\t{mean:.4f}\t{sd:.4f}\t{min(times):.4f}\t{max(times):.4f}\t{len(times)}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
