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
import time
from typing import Any

import numpy as np

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
SAMPLING_VECTOR_COUNT = 500
DEFAULT_SAMPLING_NLIST = 50
KMEANS_MAX_ITERS = 25
KMEANS_SEED = 42
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


def _parse_vector_text(text: str) -> np.ndarray:
    s = text.strip()
    if not (s.startswith("[") and s.endswith("]")):
        raise ValueError(f"Unexpected vector text: {text[:80]!r}")
    body = s[1:-1].strip()
    if not body:
        return np.zeros(0, dtype=float)
    return np.asarray([float(x) for x in body.split(",")], dtype=float)


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


def _sample_offsets(total_rows: float, sample_n: int) -> list[int]:
    total = max(1, int(total_rows))
    want = min(sample_n, total)
    if want <= 1:
        return [0]
    stride = max(1, total // want)
    offs = list(dict.fromkeys(min(total - 1, i * stride) for i in range(want)))
    if not offs:
        offs = [0]
    return offs[:want]


def _fetch_vectors_by_offsets(rel: str, offsets: list[int]) -> list[np.ndarray]:
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
    vecs: list[np.ndarray] = []
    for line in out.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split("|", 1)
        if len(parts) != 2:
            continue
        vec_txt = parts[1].strip()
        if vec_txt:
            vecs.append(_parse_vector_text(vec_txt))
    return vecs


def _fetch_sample_vectors(rel: str, sample_n: int, reltuples: float) -> tuple[list[np.ndarray], str]:
    spec = IVF_REL_SPECS[rel]
    want = max(1, min(sample_n, int(max(1.0, reltuples))))
    pct = min(100.0, max(0.01, 400.0 * want / max(1.0, float(reltuples))))
    last_method = "tablesample"
    for _ in range(5):
        sql = f"""
SELECT {spec["vector_col"]}::text
FROM {rel} TABLESAMPLE SYSTEM ({pct})
WHERE {spec["not_null"]}
LIMIT {want};
"""
        out = psql_sql(sql.strip(), tuples_only=True)
        vecs = []
        for line in out.splitlines():
            line = line.strip()
            if line:
                vecs.append(_parse_vector_text(line))
        if len(vecs) >= want:
            return vecs[:want], last_method
        pct = min(100.0, pct * 4.0)

    offsets = _sample_offsets(reltuples, want)
    return _fetch_vectors_by_offsets(rel, offsets), "offset_fallback"


def _kmeans_fit(vectors: np.ndarray, k: int, seed: int = KMEANS_SEED, max_iters: int = KMEANS_MAX_ITERS) -> tuple[np.ndarray, np.ndarray]:
    n = vectors.shape[0]
    if n == 0:
        raise ValueError("cannot cluster zero vectors")
    k = max(1, min(k, n))
    if k == n:
        return vectors.copy(), np.arange(n, dtype=int)

    rng = np.random.default_rng(seed)
    centers = np.empty((k, vectors.shape[1]), dtype=float)
    first_idx = int(rng.integers(0, n))
    centers[0] = vectors[first_idx]
    min_sq = np.sum((vectors - centers[0]) ** 2, axis=1)
    for i in range(1, k):
        total = float(np.sum(min_sq))
        if total <= 1e-18:
            centers[i:] = vectors[rng.choice(n, size=k - i, replace=False)]
            break
        probs = min_sq / total
        idx = int(rng.choice(n, p=probs))
        centers[i] = vectors[idx]
        min_sq = np.minimum(min_sq, np.sum((vectors - centers[i]) ** 2, axis=1))

    labels = np.zeros(n, dtype=int)
    for _ in range(max_iters):
        sq_dists = np.sum((vectors[:, None, :] - centers[None, :, :]) ** 2, axis=2)
        new_labels = np.argmin(sq_dists, axis=1)
        if np.array_equal(labels, new_labels):
            break
        labels = new_labels
        for j in range(k):
            mask = labels == j
            if np.any(mask):
                centers[j] = np.mean(vectors[mask], axis=0)
            else:
                farthest = int(np.argmax(np.min(sq_dists, axis=1)))
                centers[j] = vectors[farthest]
    return centers, labels


def _build_sampling_ivf_model(
    *,
    rel: str,
    lists: int,
    reltuples: float,
    sampling_nlist: int,
) -> dict[str, Any]:
    vectors, sample_method = _fetch_sample_vectors(rel, SAMPLING_VECTOR_COUNT, reltuples)
    if not vectors:
        raise RuntimeError(f"No sample vectors fetched for {rel}")
    mat = np.vstack(vectors)
    effective_nlist = max(1, min(int(sampling_nlist), lists, mat.shape[0]))
    centers, labels = _kmeans_fit(mat, effective_nlist)
    cluster_counts = np.bincount(labels, minlength=effective_nlist).astype(float)
    return {
        "sample_size": int(mat.shape[0]),
        "sampling_nlist": int(effective_nlist),
        "sample_method": sample_method,
        "centers": centers,
        "cluster_counts": cluster_counts,
    }


def _fetch_query_vector(rel: str, anchor_off: int) -> np.ndarray:
    spec = IVF_REL_SPECS[rel]
    sql = f"""
SELECT {spec["vector_col"]}::text
FROM {rel}
WHERE {spec["not_null"]}
ORDER BY {spec["order_by"]}
OFFSET {anchor_off} LIMIT 1;
"""
    out = psql_sql(sql.strip(), tuples_only=True).strip()
    if not out:
        raise RuntimeError(f"No query vector found for {rel} offset {anchor_off}")
    line = out.splitlines()[0].strip()
    if "|" in line:
        line = line.split("|", 1)[-1].strip()
    return _parse_vector_text(line)


def _sampling_query_features(
    *,
    rel: str,
    anchor_off: int,
    probes: int,
    lists: int,
    relpages: float,
    index_pages: float,
    index_tuples: float,
    sampling_models: dict[str, dict[str, Any]],
    cache: dict[tuple[str, int], np.ndarray],
) -> dict[str, float]:
    cache_key = (rel, anchor_off)
    if cache_key in cache:
        qvec = cache[cache_key]
    else:
        qvec = _fetch_query_vector(rel, anchor_off)
        cache[cache_key] = qvec

    model = sampling_models[rel]
    centers = model["centers"]
    cluster_counts = model["cluster_counts"]
    sampling_nlist = int(model["sampling_nlist"])
    sample_size = max(1, int(model["sample_size"]))
    probe_ratio = 1.0 if lists <= 0 else min(1.0, float(probes) / float(lists))
    effective_nprobe = max(1, min(sampling_nlist, int(round(probe_ratio * sampling_nlist))))
    dists = np.linalg.norm(centers - qvec, axis=1)
    nearest = np.argsort(dists)[:effective_nprobe]
    sampled_candidate_count = float(np.sum(cluster_counts[nearest]))
    sampled_candidate_share = sampled_candidate_count / float(sample_size)
    sampling_estimated_candidates = max(1.0, sampled_candidate_share * float(index_tuples))
    sampling_estimated_data_pages = max(1.0, sampled_candidate_share * float(relpages))
    sampling_estimated_index_pages = max(1.0, sampled_candidate_share * float(index_pages))
    sampling_probe_center_dist_avg = float(np.mean(dists[nearest])) if effective_nprobe > 0 else 0.0
    sampling_probe_center_dist_max = float(np.max(dists[nearest])) if effective_nprobe > 0 else 0.0
    return {
        "query_l2_norm": float(np.linalg.norm(qvec)),
        "sampling_sample_size": float(sample_size),
        "sampling_nlist": float(sampling_nlist),
        "sampling_nprobe": float(effective_nprobe),
        "sampling_candidate_share": sampled_candidate_share,
        "sampling_estimated_candidates": sampling_estimated_candidates,
        "sampling_estimated_data_pages": sampling_estimated_data_pages,
        "sampling_estimated_index_pages": sampling_estimated_index_pages,
        "sampling_probe_center_dist_avg": sampling_probe_center_dist_avg,
        "sampling_probe_center_dist_max": sampling_probe_center_dist_max,
    }


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
    sampling_estimated_candidates: float,
    sampling_estimated_data_pages: float,
    sampling_estimated_index_pages: float,
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
    estimated_candidates = max(float(limit_k), float(sampling_estimated_candidates))
    estimated_startup_pages = max(1.0, float(sampling_estimated_index_pages))
    estimated_startup_tuples = estimated_candidates
    estimated_data_pages = max(1.0, float(sampling_estimated_data_pages))
    ivf_total_cost = generic_total_cost - SEQUENTIAL_RATIO * num_index_pages * (random_page_cost - seq_page_cost)
    ivf_startup_cost = estimated_startup_pages * (
        seq_page_cost + SEQUENTIAL_RATIO * (random_page_cost - seq_page_cost)
    )
    ivf_startup_cost += estimated_startup_tuples * (cpu_index_tuple_cost + qual_op_cost)
    startup_pages = estimated_startup_pages
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
        "estimated_data_pages": estimated_data_pages,
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
    ap.add_argument("--out", default=os.path.join(os.path.dirname(__file__), "data", "ivf_scan_samples_new.jsonl"))
    ap.add_argument("--repeats", type=int, default=1)
    ap.add_argument("--target", type=int, default=500)
    ap.add_argument("--sampling-nlist", type=int, default=DEFAULT_SAMPLING_NLIST)
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    stats = load_table_stats()
    index_stats = load_index_stats()
    cost_gucs = _load_cost_gucs()
    ivf_lists = _load_ivf_lists_by_index()
    sampling_models: dict[str, dict[str, Any]] = {}
    query_cache: dict[tuple[str, int], np.ndarray] = {}
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
            if rel not in sampling_models:
                try:
                    sampling_models[rel] = _build_sampling_ivf_model(
                        rel=rel,
                        lists=lists,
                        reltuples=tuples,
                        sampling_nlist=args.sampling_nlist,
                    )
                except Exception as e:
                    print(f"[warn] {tag}: sampling-IVF build failed for {rel}: {e}", file=sys.stderr)
                    continue
            n_index_quals = _field_item_count(cand.get("Index Cond"))
            n_orderbys = _field_item_count(cand.get("Order By"))
            limit_k = _parse_limit(sql)
            anchor_offset = _parse_anchor_offset(sql)
            feature_extract_start_ns = time.perf_counter_ns()
            sampling_feature_start_ns = feature_extract_start_ns
            sampling_features = _sampling_query_features(
                rel=rel,
                anchor_off=anchor_offset,
                probes=int(knobs["ivfflat.probes"]),
                lists=lists,
                relpages=pages,
                index_pages=index_pages,
                index_tuples=index_tuples,
                sampling_models=sampling_models,
                cache=query_cache,
            )
            sampling_feature_time_us = (time.perf_counter_ns() - sampling_feature_start_ns) / 1000.0
            estimate_start_ns = time.perf_counter_ns()
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
                sampling_estimated_candidates=sampling_features["sampling_estimated_candidates"],
                sampling_estimated_data_pages=sampling_features["sampling_estimated_data_pages"],
                sampling_estimated_index_pages=sampling_features["sampling_estimated_index_pages"],
                cost_gucs=cost_gucs,
            )
            estimate_feature_time_us = (time.perf_counter_ns() - estimate_start_ns) / 1000.0
            feature_extract_total_us = (time.perf_counter_ns() - feature_extract_start_ns) / 1000.0

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
                "feature_extract_total_us": feature_extract_total_us,
                "sampling_feature_time_us": sampling_feature_time_us,
                "estimate_feature_time_us": estimate_feature_time_us,
                **_node_buffer_stats(cand),
                **cost_gucs,
                **sampling_features,
                **estimates,
            }
            fout.write(json.dumps(row, ensure_ascii=False) + "\n")
            n_ok += 1
            print(tag, ex, "ms")

    print("wrote", args.out, "count", n_ok)
    if n_ok < args.target:
        print(f"[warn] only {n_ok} ivf-scan samples (<{args.target}).", file=sys.stderr)


if __name__ == "__main__":
    main()
