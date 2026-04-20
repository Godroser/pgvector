#!/usr/bin/env python3
"""
Per-operator extraction of target node exclusive_ms and full transaction prefixes.
Mirrors cost_fit collectors (single-connection), but prefixes are merged into one
tx_prefix so each concurrent psql session is self-contained.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from parallel_common import cost_fit_dir, session_prelude_sql

# Import from cost_fit after parallel_common adjusted sys.path
from fit_common import exclusive_total_time, walk_plans  # noqa: E402
from workloads import (  # noqa: E402
    agg_workloads,
    hashjoin_workloads,
    index_scan_workloads,
    ivf_scan_workloads,
    mergejoin_workloads,
    scan_workloads,
    sort_workloads,
)

WorkloadItem = Union[Tuple[str, str], Tuple[str, str, Dict[str, Any]]]

MERGE_GUC = """SET LOCAL enable_hashjoin TO off;
SET LOCAL enable_nestloop TO off;
SET LOCAL enable_mergejoin TO on;"""

HASH_FORCE = """SET LOCAL enable_nestloop TO off;
SET LOCAL enable_mergejoin TO off;"""

FORCE_INDEX = "SET LOCAL enable_seqscan TO off;"

SORT_INDEX_OFF = """SET LOCAL enable_indexscan TO off;
SET LOCAL enable_bitmapscan TO off;
SET LOCAL enable_indexonlyscan TO off;"""


def _prelude_plus(*parts: str) -> str:
    p = session_prelude_sql()
    body = "\n".join(x.strip() for x in parts if x and x.strip())
    if body:
        return p + "\n" + body + "\n"
    return p + "\n"


def tx_prefix_sort() -> str:
    return _prelude_plus(SORT_INDEX_OFF)


def tx_prefix_mergejoin() -> str:
    return _prelude_plus(MERGE_GUC)


def tx_prefix_hashjoin() -> str:
    return _prelude_plus(HASH_FORCE)


def tx_prefix_scan() -> str:
    return _prelude_plus()


def tx_prefix_index_scan() -> str:
    return _prelude_plus(FORCE_INDEX)


def tx_prefix_agg() -> str:
    return _prelude_plus()


def tx_prefix_ivf(knobs: Dict[str, Any]) -> str:
    lines = [session_prelude_sql().strip(), FORCE_INDEX]
    for k in sorted(knobs.keys()):
        lines.append(f"SET LOCAL {k} TO {knobs[k]};")
    return "\n".join(lines) + "\n"


# --- extractors: plan root -> exclusive_ms or None ---


def extract_sort(root: Dict[str, Any]) -> Optional[float]:
    for node in walk_plans(root["Plan"]):
        nt = node.get("Node Type")
        if nt in ("Sort", "Incremental Sort"):
            return exclusive_total_time(node)
    return None


def extract_merge_join(root: Dict[str, Any]) -> Optional[float]:
    for node in walk_plans(root["Plan"]):
        if node.get("Node Type") == "Merge Join":
            kids = node.get("Plans") or []
            if len(kids) >= 2:
                return exclusive_total_time(node)
    return None


def extract_hash_join(root: Dict[str, Any]) -> Optional[float]:
    for node in walk_plans(root["Plan"]):
        if node.get("Node Type") == "Hash Join":
            kids = node.get("Plans") or []
            if len(kids) >= 2:
                return exclusive_total_time(node)
    return None


def extract_first_seq_scan(root: Dict[str, Any]) -> Optional[float]:
    for node in walk_plans(root["Plan"]):
        if node.get("Node Type") == "Seq Scan":
            return exclusive_total_time(node)
    return None


def extract_index_scan_primary(root: Dict[str, Any]) -> Optional[float]:
    """First Index Scan or Index Only Scan (same preference as 03_collect_index_scan_new)."""
    for node in walk_plans(root["Plan"]):
        nt = node.get("Node Type")
        if nt in ("Index Scan", "Index Only Scan"):
            return exclusive_total_time(node)
    return None


AGG_TYPES = frozenset({"Hash Aggregate", "GroupAggregate", "Aggregate"})


def extract_aggregate(root: Dict[str, Any]) -> Optional[float]:
    plan = root["Plan"]
    if plan.get("Node Type") in AGG_TYPES and "Partial" not in (plan.get("Node Type") or ""):
        return exclusive_total_time(plan)
    for node in walk_plans(plan):
        nt = node.get("Node Type") or ""
        if nt in AGG_TYPES and "Partial" not in nt:
            return exclusive_total_time(node)
    return None


def extract_ivf_index_scan(root: Dict[str, Any]) -> Optional[float]:
    for node in walk_plans(root["Plan"]):
        if node.get("Node Type") != "Index Scan":
            continue
        iname = (node.get("Index Name") or "").lower()
        rel = (node.get("Relation Name") or "").lower()
        if ("ivf" in iname or "ivfflat" in iname) and rel in ("part", "partsupp"):
            return exclusive_total_time(node)
    return None


@dataclass(frozen=True)
class OperatorSpec:
    name: str
    data_file: str  # under cost_fit/data/
    workloads: Callable[[int], List[WorkloadItem]]
    tx_prefix: Callable[..., str]
    extract: Callable[[Dict[str, Any]], Optional[float]]
    tx_prefix_needs_knobs: bool = False


def normalize_workloads(raw: List[WorkloadItem]) -> List[Tuple[str, str, Dict[str, Any]]]:
    out: List[Tuple[str, str, Dict[str, Any]]] = []
    for item in raw:
        if len(item) == 2:
            tag, sql = item[0], item[1]
            out.append((tag, sql, {}))
        else:
            tag, sql, knobs = item[0], item[1], dict(item[2])
            out.append((tag, sql, knobs))
    return out


def get_operator_spec(name: str) -> OperatorSpec:
    n = name.lower().strip()
    cost_fit = cost_fit_dir()
    if n in ("sort",):
        return OperatorSpec(
            name="sort",
            data_file=os.path.join(cost_fit, "data", "sort_samples_new.jsonl"),
            workloads=lambda t: normalize_workloads(sort_workloads(t)),
            tx_prefix=lambda **_: tx_prefix_sort(),
            extract=extract_sort,
        )
    if n in ("mergejoin", "merge_join", "merge"):
        return OperatorSpec(
            name="mergejoin",
            data_file=os.path.join(cost_fit, "data", "mergejoin_samples_new.jsonl"),
            workloads=lambda t: normalize_workloads(mergejoin_workloads(t)),
            tx_prefix=lambda **_: tx_prefix_mergejoin(),
            extract=extract_merge_join,
        )
    if n in ("hashjoin", "hash_join", "hj"):
        return OperatorSpec(
            name="hashjoin",
            data_file=os.path.join(cost_fit, "data", "hashjoin_samples_new.jsonl"),
            workloads=lambda t: normalize_workloads(hashjoin_workloads(t)),
            tx_prefix=lambda **_: tx_prefix_hashjoin(),
            extract=extract_hash_join,
        )
    if n in ("scan", "seqscan", "seq_scan"):
        return OperatorSpec(
            name="scan",
            data_file=os.path.join(cost_fit, "data", "scan_samples_new.jsonl"),
            workloads=lambda t: normalize_workloads(scan_workloads(t)),
            tx_prefix=lambda **_: tx_prefix_scan(),
            extract=extract_first_seq_scan,
        )
    if n in ("index_scan", "indexscan", "idx"):
        return OperatorSpec(
            name="index_scan",
            data_file=os.path.join(cost_fit, "data", "index_scan_samples_new.jsonl"),
            workloads=lambda t: normalize_workloads(index_scan_workloads(t)),
            tx_prefix=lambda **_: tx_prefix_index_scan(),
            extract=extract_index_scan_primary,
        )
    if n in ("agg", "aggregate"):
        return OperatorSpec(
            name="agg",
            data_file=os.path.join(cost_fit, "data", "agg_samples_new.jsonl"),
            workloads=lambda t: normalize_workloads(agg_workloads(t)),
            tx_prefix=lambda **_: tx_prefix_agg(),
            extract=extract_aggregate,
        )
    if n in ("ivf", "ivf_scan", "vector_ivf", "ivf-scan"):
        return OperatorSpec(
            name="ivf_scan",
            data_file=os.path.join(cost_fit, "data", "ivf_scan_samples.jsonl"),
            workloads=lambda t: normalize_workloads(ivf_scan_workloads(t)),
            tx_prefix=lambda knobs, **_: tx_prefix_ivf(knobs),
            extract=extract_ivf_index_scan,
            tx_prefix_needs_knobs=True,
        )
    raise ValueError(
        f"Unknown operator {name!r}. Choose from: "
        "sort, mergejoin, hashjoin, scan, index_scan, agg, ivf_scan"
    )


def list_operator_names() -> List[str]:
    return ["sort", "mergejoin", "hashjoin", "scan", "index_scan", "agg", "ivf_scan"]
