#!/usr/bin/env python3
"""
Collect IVFFlat-backed pgvector ANN (ORDER BY <-> … LIMIT) Index Scan samples.
Requires prepare_vector_indexes_ivf.sql (no concurrent HNSW on same columns).
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import statistics
import sys
from typing import Any

from fit_common import (
    exclusive_total_time,
    explain_analyze_json,
    load_index_stats,
    load_table_stats,
    psql_sql,
    walk_plans,
)
from workloads import ivf_scan_workloads

FORCE_INDEX = "SET LOCAL enable_seqscan TO off;"
SEQUENTIAL_RATIO = 0.5
IVF_REF_OFFSETS = {
    "part": [0, 5, 29, 131, 701, 1999],
    "partsupp": [0, 11, 127, 997, 4501, 15001],
}
IVF_REL_SPECS = {
    "part": {
        "vector_col": "text_embedding",
        "order_by": "p_partkey",
        "not_null": "text_embedding IS NOT NULL",
        "vector_type": "vector",
    },
    "partsupp": {
        "vector_col": "ps_text_embedding",
        "order_by": "ps_partkey, ps_suppkey",
        "not_null": "ps_text_embedding IS NOT NULL",
        "vector_type": "vector",
    },
}


def _session_prelude() -> str:
    path = os.path.join(os.path.dirname(__file__), "session_prelude.sql")
    with open(path, encoding="utf-8") as f:
        return f.read().strip() + "\n"


def _tx_prefix(knobs: dict) -> str:
    lines = [_session_prelude(), FORCE_INDEX]
    for k in sorted(knobs.keys()):
        lines.append(f"SET LOCAL {k} TO {knobs[k]};")
    return "\n".join(lines) + "\n"


def _parse_limit(sql: str) -> int:
    m = re.search(r"\bLIMIT\s+(\d+)\s*;", sql, re.I | re.DOTALL)
    return int(m.group(1)) if m else 0


def _parse_anchor_offset(sql: str) -> int:
    m = re.search(r"\bOFFSET\s+(\d+)\s+LIMIT\s+1\)", sql, re.I | re.DOTALL)
    return int(m.group(1)) if m else 0


def _field_item_count(v) -> float:
    if not v:
        return 0.0
    if isinstance(v, list):
        return float(len([x for x in v if x is not None]))
    return 1.0


def _load_cost_gucs() -> dict:
    sql = """
SELECT
  current_setting('seq_page_cost')::float,
  current_setting('random_page_cost')::float,
  current_setting('cpu_index_tuple_cost')::float,
  current_setting('cpu_operator_cost')::float;
"""
    out = psql_sql(sql.strip(), tuples_only=True).strip()
    parts = out.split("|")
    if len(parts) != 4:
        raise RuntimeError(f"Unexpected planner GUC row: {out!r}")
    return {
        "seq_page_cost": float(parts[0]),
        "random_page_cost": float(parts[1]),
        "cpu_index_tuple_cost": float(parts[2]),
        "cpu_operator_cost": float(parts[3]),
    }


def _load_ivf_lists_by_index() -> dict[str, int]:
    sql = """
SELECT c.relname,
       COALESCE(MAX(CASE WHEN o.option_name = 'lists' THEN o.option_value END), '0') AS lists
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_am am ON am.oid = c.relam
LEFT JOIN LATERAL pg_options_to_table(c.reloptions) o ON true
WHERE n.nspname = 'public'
  AND c.relkind IN ('i', 'I')
  AND am.amname = 'ivfflat'
GROUP BY c.relname;
"""
    out = psql_sql(sql.strip(), tuples_only=True)
    d: dict[str, int] = {}
    for line in out.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split("|")
        if len(parts) != 2:
            continue
        d[parts[0].lower()] = int(parts[1] or 0)
    return d


def _quote_sql_literal(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def _load_reference_vectors() -> dict[str, list[str]]:
    refs: dict[str, list[str]] = {}
    for rel, offsets in IVF_REF_OFFSETS.items():
        spec = IVF_REL_SPECS[rel]
        values = ", ".join(f"({i}, {off})" for i, off in enumerate(offsets))
        sql = f"""
WITH offs(ord, off) AS (
  VALUES {values}
)
SELECT ord,
       (
         SELECT {spec["vector_col"]}::text
         FROM {rel}
         WHERE {spec["not_null"]}
         ORDER BY {spec["order_by"]}
         OFFSET off LIMIT 1
       ) AS vec_txt
FROM offs
ORDER BY ord;
"""
        out = psql_sql(sql.strip(), tuples_only=True)
        vecs: list[str] = []
        for line in out.splitlines():
            line = line.strip()
            if not line:
                continue
            parts = line.split("|", 1)
            if len(parts) != 2:
                continue
            vec_txt = parts[1].strip()
            if vec_txt:
                vecs.append(vec_txt)
        refs[rel] = vecs
    return refs


def _query_vector_features(
    *,
    rel: str,
    anchor_off: int,
    ref_vectors: dict[str, list[str]],
    cache: dict[tuple[str, int], dict[str, float]],
) -> dict[str, float]:
    cache_key = (rel, anchor_off)
    if cache_key in cache:
        return cache[cache_key]

    spec = IVF_REL_SPECS[rel]
    refs = ref_vectors.get(rel, [])
    if not refs:
        features = {
            "anchor_offset": float(anchor_off),
            "query_l2_norm": 0.0,
            "query_ref_dist_min": 0.0,
            "query_ref_dist_max": 0.0,
            "query_ref_dist_avg": 0.0,
            "query_ref_dist_std": 0.0,
        }
        cache[cache_key] = features
        return features

    dist_cols = []
    for i, vec_txt in enumerate(refs, start=1):
        lit = _quote_sql_literal(vec_txt)
        dist_cols.append(f"(q.v <-> {lit}::{spec['vector_type']})::float8 AS d{i}")
    sql = f"""
WITH q AS (
  SELECT {spec["vector_col"]} AS v
  FROM {rel}
  WHERE {spec["not_null"]}
  ORDER BY {spec["order_by"]}
  OFFSET {anchor_off} LIMIT 1
)
SELECT vector_norm(q.v)::float8,
       {", ".join(dist_cols)}
FROM q;
"""
    out = psql_sql(sql.strip(), tuples_only=True).strip()
    parts = out.split("|") if out else []
    norm = float(parts[0]) if parts and parts[0] else 0.0
    dists = [float(x) for x in parts[1:] if x]
    avg = statistics.fmean(dists) if dists else 0.0
    std = statistics.pstdev(dists) if len(dists) > 1 else 0.0
    features = {
        "anchor_offset": float(anchor_off),
        "query_l2_norm": norm,
        "query_ref_dist_min": min(dists) if dists else 0.0,
        "query_ref_dist_max": max(dists) if dists else 0.0,
        "query_ref_dist_avg": avg,
        "query_ref_dist_std": std,
    }
    cache[cache_key] = features
    return features


def _node_buffer_stats(node: dict[str, Any]) -> dict[str, float]:
    keys = (
        "Shared Hit Blocks",
        "Shared Read Blocks",
        "Shared Dirtied Blocks",
        "Shared Written Blocks",
        "Local Hit Blocks",
        "Local Read Blocks",
        "Local Dirtied Blocks",
        "Local Written Blocks",
        "Temp Read Blocks",
        "Temp Written Blocks",
    )
    out: dict[str, float] = {}
    for key in keys:
        out[key.lower().replace(" ", "_")] = float(node.get(key) or 0.0)
    return out


def _estimate_generic_ivf_costs(
    *,
    plan_rows: float,
    reltuples: float,
    relpages: float,
    index_tuples: float,
    index_pages: float,
    probes: int,
    lists: int,
    n_index_quals: float,
    n_orderbys: float,
    limit_k: int,
    cost_gucs: dict,
) -> dict:
    random_page_cost = cost_gucs["random_page_cost"]
    seq_page_cost = cost_gucs["seq_page_cost"]
    cpu_index_tuple_cost = cost_gucs["cpu_index_tuple_cost"]
    cpu_operator_cost = cost_gucs["cpu_operator_cost"]
    idx_total_tuples = max(1.0, float(index_tuples))
    idx_total_pages = max(1.0, float(index_pages))
    heap_tuples = max(1.0, float(reltuples))
    # genericcostestimate uses index selectivity and per-scan tuples; ivf path has num_sa_scans=1.
    s = max(0.0, float(plan_rows) / heap_tuples)
    num_index_tuples = float(round(s * heap_tuples))
    num_index_tuples = min(idx_total_tuples, max(1.0, num_index_tuples))
    num_index_pages = max(1.0, math.ceil(num_index_tuples * idx_total_pages / idx_total_tuples))
    qual_op_cost = cpu_operator_cost * (float(n_index_quals) + float(n_orderbys))
    generic_io_cost = num_index_pages * random_page_cost
    generic_cpu_cost = num_index_tuples * (cpu_index_tuple_cost + qual_op_cost)
    generic_total_cost = generic_io_cost + generic_cpu_cost

    ratio = 1.0 if lists <= 0 else min(1.0, float(probes) / float(lists))
    tuples_per_list = idx_total_tuples / max(1.0, float(lists))
    pages_per_list = idx_total_pages / max(1.0, float(lists))
    estimated_candidates = ratio * idx_total_tuples
    estimated_startup_pages = ratio * num_index_pages
    estimated_startup_tuples = max(float(limit_k), ratio * num_index_tuples)
    ivf_total_cost = generic_total_cost - SEQUENTIAL_RATIO * num_index_pages * (random_page_cost - seq_page_cost)
    ivf_startup_cost = ivf_total_cost * ratio
    startup_pages = num_index_pages * ratio
    if startup_pages > relpages and ratio < 0.5:
        ivf_startup_cost -= (1.0 - SEQUENTIAL_RATIO) * startup_pages * (random_page_cost - seq_page_cost)
        ivf_startup_cost -= (startup_pages - relpages) * seq_page_cost

    return {
        "index_selectivity_est": s,
        "num_index_tuples_est": num_index_tuples,
        "num_index_pages_est": num_index_pages,
        "generic_io_cost_est": generic_io_cost,
        "generic_cpu_cost_est": generic_cpu_cost,
        "generic_total_cost_est": generic_total_cost,
        "ivf_ratio_est": ratio,
        "probes_over_lists": ratio,
        "estimated_tuples_per_list": tuples_per_list,
        "estimated_pages_per_list": pages_per_list,
        "estimated_candidates": estimated_candidates,
        "estimated_startup_pages": estimated_startup_pages,
        "estimated_startup_tuples": estimated_startup_tuples,
        "ivf_total_cost_est": ivf_total_cost,
        "ivf_startup_cost_est": ivf_startup_cost,
    }


def _pick_ivf_scan(root: dict) -> dict | None:
    plan = root["Plan"]
    for node in walk_plans(plan):
        if node.get("Node Type") != "Index Scan":
            continue
        iname = (node.get("Index Name") or "").lower()
        rel = (node.get("Relation Name") or "").lower()
        if ("ivf" in iname or "ivfflat" in iname) and rel in ("part", "partsupp"):
            return node
    return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.path.join(os.path.dirname(__file__), "data", "ivf_scan_samples.jsonl"))
    ap.add_argument("--repeats", type=int, default=1)
    ap.add_argument("--target", type=int, default=500)
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    stats = load_table_stats()
    index_stats = load_index_stats()
    cost_gucs = _load_cost_gucs()
    ivf_lists = _load_ivf_lists_by_index()
    ref_vectors = _load_reference_vectors()
    query_cache: dict[tuple[str, int], dict[str, float]] = {}
    workloads = ivf_scan_workloads(args.target)
    if args.limit:
        workloads = workloads[: args.limit]

    n_ok = 0
    with open(args.out, "w", encoding="utf-8") as fout:
        for tag, sql, knobs in workloads:
            runs = []
            for _ in range(args.repeats):
                try:
                    root = explain_analyze_json(sql, tx_prefix=_tx_prefix(knobs))
                except Exception as e:
                    print(f"[fail] {tag}: {e}", file=sys.stderr)
                    continue
                node = _pick_ivf_scan(root)
                runs.append(node)
            if not runs or all(n is None for n in runs):
                print(f"[warn] {tag}: no IVFFlat Index Scan in plan", file=sys.stderr)
                continue
            cand = next(n for n in runs if n is not None)
            rel = (cand.get("Relation Name") or "").lower()
            if rel not in stats:
                print(f"[warn] {tag}: bad relation", file=sys.stderr)
                continue
            tuples, pages = stats[rel]
            index_name = str(cand.get("Index Name") or "")
            index_name_l = index_name.lower()
            if index_name_l not in index_stats:
                print(f"[warn] {tag}: index stats missing for {index_name!r}", file=sys.stderr)
                continue
            index_tuples, index_pages = index_stats[index_name_l]
            lists = int(ivf_lists.get(index_name_l, 0))
            if lists <= 0:
                print(f"[warn] {tag}: ivfflat lists missing for {index_name!r}", file=sys.stderr)
                continue
            n_index_quals = _field_item_count(cand.get("Index Cond"))
            n_orderbys = _field_item_count(cand.get("Order By"))
            limit_k = _parse_limit(sql)
            anchor_offset = _parse_anchor_offset(sql)
            query_features = _query_vector_features(
                rel=rel,
                anchor_off=anchor_offset,
                ref_vectors=ref_vectors,
                cache=query_cache,
            )
            estimates = _estimate_generic_ivf_costs(
                plan_rows=float(cand.get("Plan Rows") or 0),
                reltuples=tuples,
                relpages=pages,
                index_tuples=index_tuples,
                index_pages=index_pages,
                probes=int(knobs["ivfflat.probes"]),
                lists=lists,
                n_index_quals=n_index_quals,
                n_orderbys=n_orderbys,
                limit_k=limit_k,
                cost_gucs=cost_gucs,
            )

            def match_ms(n):
                return exclusive_total_time(n) if n is not None else None

            vals = [match_ms(n) for n in runs]
            vals = [v for v in vals if v is not None]
            if not vals:
                continue
            ex = float(statistics.median(vals))
            row = {
                "tag": tag,
                "sql": sql,
                "relation": rel,
                "index_name": index_name,
                "node_type": "Index Scan (ivfflat)",
                "relpages": pages,
                "reltuples": tuples,
                "index_relpages": index_pages,
                "index_reltuples": index_tuples,
                "exclusive_ms": ex,
                "planner_startup_cost": float(cand.get("Startup Cost") or 0.0),
                "planner_total_cost": float(cand.get("Total Cost") or 0.0),
                "plan_rows": float(cand.get("Plan Rows") or 0),
                "actual_rows": float(cand.get("Actual Rows") or 0),
                "plan_width": int(cand.get("Plan Width") or 0),
                "limit_k": limit_k,
                "probes": int(knobs["ivfflat.probes"]),
                "lists": lists,
                "n_index_quals": n_index_quals,
                "n_orderbys": n_orderbys,
                **_node_buffer_stats(cand),
                **cost_gucs,
                **query_features,
                **estimates,
            }
            fout.write(json.dumps(row, ensure_ascii=False) + "\n")
            n_ok += 1
            print(tag, ex, "ms")

    print("wrote", args.out, "count", n_ok)
    if n_ok < 500:
        print(f"[warn] only {n_ok} ivf-scan samples (<200).", file=sys.stderr)


if __name__ == "__main__":
    main()
