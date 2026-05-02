#!/usr/bin/env python3
"""
Mixed concurrent workloads: run **different** SQL statements (different operator families
and/or different tags) in one parallel batch, then record the primary operator exclusive
time for each query as if it were the "focus" query.

Baselines: solo exclusive_ms per tag are read from cost_fit/data/*_samples*.jsonl (merged).
Use --measure-solo-missing to fill missing tags with one sequential EXPLAIN per tag.

Output: data/mix_parallel_samples.jsonl — one row per (batch, focus_index).

Does not modify dev/cost_fit or dev/cost_fit_multi_parallel collectors.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Sequence, Tuple

from mix_import_paths import ensure_paths

ensure_paths()

from mix_features import OPERATOR_KEYS, counts_and_solo_sums  # noqa: E402
from operator_parallel import (  # noqa: E402
    OperatorSpec,
    get_operator_spec,
    list_operator_names,
    normalize_workloads,
)
from parallel_common import explain_one, load_single_baselines  # noqa: E402


def resolve_tx_prefix(spec: OperatorSpec, knobs: Dict[str, Any]) -> str:
    if spec.tx_prefix_needs_knobs:
        return spec.tx_prefix(knobs=knobs)
    return spec.tx_prefix()


def load_merged_solo_baselines(enabled_ops: Sequence[str]) -> Dict[str, float]:
    merged: Dict[str, float] = {}
    for op in enabled_ops:
        spec = get_operator_spec(op)
        part = load_single_baselines(spec.data_file)
        for tag, ms in part.items():
            merged[str(tag)] = float(ms)
    return merged


def build_pools(enabled_ops: Sequence[str], target_per_op: int) -> Dict[str, List[Tuple[str, str, Dict[str, Any]]]]:
    pools: Dict[str, List[Tuple[str, str, Dict[str, Any]]]] = {}
    for op in enabled_ops:
        spec = get_operator_spec(op)
        raw = spec.workloads(target_per_op)
        pools[op] = normalize_workloads(raw)
    return pools


def measure_solo(spec: OperatorSpec, tag: str, sql: str, knobs: Dict[str, Any]) -> Optional[float]:
    tx = resolve_tx_prefix(spec, knobs)
    try:
        root = explain_one(sql, tx)
    except Exception as e:
        print(f"[fail] solo EXPLAIN {tag}: {e}", file=sys.stderr)
        return None
    return spec.extract(root)


def run_mixed_batch(
    items: List[Tuple[str, str, Dict[str, Any], OperatorSpec]],
) -> List[Optional[float]]:
    """Concurrent EXPLAIN ANALYZE; one subprocess per item. Returns exclusive_ms per item order."""

    def work(
        it: Tuple[str, str, Dict[str, Any], OperatorSpec],
    ) -> Optional[float]:
        _tag, sql, knobs, spec = it
        tx = resolve_tx_prefix(spec, knobs)
        try:
            root = explain_one(sql, tx)
        except Exception as e:
            print(f"[fail] mixed EXPLAIN {_tag}: {e}", file=sys.stderr)
            return None
        return spec.extract(root)

    n = len(items)
    out: List[Optional[float]] = [None] * n
    with ThreadPoolExecutor(max_workers=n) as pool:
        futs = {pool.submit(work, it): i for i, it in enumerate(items)}
        for fut in as_completed(futs):
            i = futs[fut]
            out[i] = fut.result()
    return out


def parse_ops(arg: str, all_ops: Sequence[str]) -> List[str]:
    if arg.strip().lower() in ("all", "*"):
        return list(all_ops)
    out = []
    for p in arg.split(","):
        p = p.strip().lower()
        if not p:
            continue
        if p not in all_ops:
            raise SystemExit(f"unknown op {p!r}; choose from {list(all_ops)}")
        out.append(p)
    if not out:
        raise SystemExit("empty --ops")
    return out


def sample_batch_ops(rng: random.Random, pool_ops: Sequence[str], degree: int) -> List[str]:
    """Prefer distinct operator families; if degree > len(pool_ops), sample with replacement."""
    if degree <= len(pool_ops):
        return rng.sample(list(pool_ops), k=degree)
    out = []
    for _ in range(degree):
        out.append(rng.choice(list(pool_ops)))
    return out


def pick_workload(
    rng: random.Random,
    pools: Dict[str, List[Tuple[str, str, Dict[str, Any]]]],
    op: str,
    forbidden_tags: set,
    max_tries: int = 80,
) -> Optional[Tuple[str, str, Dict[str, Any], OperatorSpec]]:
    spec = get_operator_spec(op)
    wl = pools.get(op) or []
    if not wl:
        return None
    for _ in range(max_tries):
        tag, sql, knobs = rng.choice(wl)
        if tag not in forbidden_tags:
            return (tag, sql, knobs, spec)
    # fallback: allow duplicate tag if pool tiny
    tag, sql, knobs = rng.choice(wl)
    return (tag, sql, knobs, spec)


def main() -> None:
    ap = argparse.ArgumentParser(description="Collect mixed-SQL concurrent operator times.")
    ap.add_argument(
        "--out",
        default=os.path.join(os.path.dirname(__file__), "data", "mix_parallel_samples.jsonl"),
    )
    ap.add_argument("--batches", type=int, default=200, help="Number of random concurrent batches")
    ap.add_argument("--degree-min", type=int, default=2)
    ap.add_argument("--degree-max", type=int, default=5)
    ap.add_argument("--target-per-op", type=int, default=80, help="Per-family workload pool size")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument(
        "--ops",
        default="all",
        help="Comma-separated operator families or 'all'",
    )
    ap.add_argument(
        "--measure-solo-missing",
        action="store_true",
        help="Sequential EXPLAIN to measure solo_ms when tag is missing from cost_fit jsonl",
    )
    ap.add_argument(
        "--require-all-solo",
        action="store_true",
        help="Skip entire batch if any selected tag has no solo_ms (after optional measure)",
    )
    args = ap.parse_args()

    rng = random.Random(args.seed)
    all_ops = list_operator_names()
    enabled = parse_ops(args.ops, all_ops)
    pools = build_pools(enabled, args.target_per_op)
    solo_cache = load_merged_solo_baselines(enabled)

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    n_rows = 0
    batch_num = 0

    with open(args.out, "w", encoding="utf-8") as fout:
        for _ in range(args.batches):
            batch_num += 1
            d = rng.randint(args.degree_min, args.degree_max)
            op_seq = sample_batch_ops(rng, enabled, d)
            forbidden: set = set()
            items: List[Tuple[str, str, Dict[str, Any], OperatorSpec]] = []
            tags: List[str] = []
            op_names: List[str] = []
            for op in op_seq:
                picked = pick_workload(rng, pools, op, forbidden)
                if picked is None:
                    print(f"[warn] batch {batch_num}: empty pool for {op}", file=sys.stderr)
                    items = []
                    break
                tag, sql, knobs, spec = picked
                forbidden.add(tag)
                items.append((tag, sql, knobs, spec))
                tags.append(tag)
                op_names.append(spec.name)

            if not items:
                continue

            solos: List[float] = []
            skip_batch = False
            for tag, sql, knobs, spec in items:
                ms = solo_cache.get(tag)
                if ms is None and args.measure_solo_missing:
                    print(f"[progress] measure-solo {tag!r} ...", flush=True)
                    v = measure_solo(spec, tag, sql, knobs)
                    if v is not None:
                        ms = float(v)
                        solo_cache[tag] = ms
                if ms is None:
                    if args.require_all_solo:
                        skip_batch = True
                        break
                    print(f"[warn] skip batch {batch_num}: no solo_ms for tag {tag!r}", file=sys.stderr)
                    skip_batch = True
                    break
                solos.append(float(ms))

            if skip_batch:
                continue

            batch_id = str(uuid.uuid4())
            t0 = time.perf_counter()
            parallel_ms = run_mixed_batch(items)
            wall_ms = (time.perf_counter() - t0) * 1000.0

            if any(x is None for x in parallel_ms):
                print(f"[warn] batch {batch_num}: incomplete parallel extracts", file=sys.stderr)
                continue

            n_T, sum_T = counts_and_solo_sums(op_names, solos)
            total_solo = float(sum(solos))

            for focus_idx, (tag, _sql, _knobs, spec) in enumerate(items):
                focus_op = spec.name
                focus_solo = solos[focus_idx]
                peer_sum = total_solo - focus_solo
                y = float(parallel_ms[focus_idx] or 0.0)

                row: Dict[str, Any] = {
                    "batch_id": batch_id,
                    "batch_num": batch_num,
                    "degree": d,
                    "wall_clock_batch_ms": wall_ms,
                    "operators_in_batch": op_names,
                    "tags_in_batch": tags,
                    "solo_ms_by_tag": {t: s for t, s in zip(tags, solos)},
                    "focus_index": focus_idx,
                    "focus_operator": focus_op,
                    "focus_tag": tag,
                    "focus_solo_ms": focus_solo,
                    "sum_solo_peers_ms": peer_sum,
                    "total_solo_batch_ms": total_solo,
                    "parallel_exclusive_ms": y,
                    "n_sort": n_T["sort"],
                    "n_mergejoin": n_T["mergejoin"],
                    "n_hashjoin": n_T["hashjoin"],
                    "n_scan": n_T["scan"],
                    "n_index_scan": n_T["index_scan"],
                    "n_agg": n_T["agg"],
                    "n_ivf_scan": n_T["ivf_scan"],
                    "solo_sum_sort_ms": sum_T["sort"],
                    "solo_sum_mergejoin_ms": sum_T["mergejoin"],
                    "solo_sum_hashjoin_ms": sum_T["hashjoin"],
                    "solo_sum_scan_ms": sum_T["scan"],
                    "solo_sum_index_scan_ms": sum_T["index_scan"],
                    "solo_sum_agg_ms": sum_T["agg"],
                    "solo_sum_ivf_scan_ms": sum_T["ivf_scan"],
                }
                fout.write(json.dumps(row, ensure_ascii=False) + "\n")
                n_rows += 1

            print(
                f"[progress] batch {batch_num} degree={d} ops={op_names} focus0={tags[0]} wall={wall_ms:.1f}ms",
                flush=True,
            )

    print("wrote", args.out, "rows", n_rows, flush=True)


if __name__ == "__main__":
    main()
