#!/usr/bin/env bash
# Partitioned HNSW cost_fit only (not part of run_all.sh).
# 1) Clones part/partsupp into 100-way range-partitioned tables, copies all rows.
# 2) Builds HNSW on partition children idx 16, 49, 82 only (6 indexes total).
# 3) Collects ANN timings with queries scoped to a single key range per statement.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HP="${ROOT}/hnsw_partition"
export PGHOST="${PGHOST:-127.0.0.1}"
export PGPORT="${PGPORT:-5432}"
export PGUSER="${PGUSER:-dzh}"
export PGDATABASE="${PGDATABASE:-tpch10}"
export PSQL="${PSQL:-/data/dzh/postgresql/bin/psql}"
if [[ ! -x "$PSQL" ]]; then PSQL="$(command -v psql)"; fi

echo "=== [hnsw_partition] create 100 partitions + load from part/partsupp (long) ==="
"$PSQL" -d "$PGDATABASE" -v ON_ERROR_STOP=1 -f "${HP}/00_create_partitioned_tables.sql"

echo "=== [hnsw_partition] HNSW on partitions 16, 49, 82 only ==="
"$PSQL" -d "$PGDATABASE" -v ON_ERROR_STOP=1 -f "${HP}/01_create_hnsw_on_selected_partitions.sql"

echo "=== [hnsw_partition] Python deps ==="
pip install -q -r "${ROOT}/requirements.txt"

echo "=== [hnsw_partition] collect + fit ==="
cd "$ROOT"
python3 hnsw_partition/collect_hnsw_partition.py
python3 hnsw_partition/fit_hnsw_partition.py || echo "skip if <50 samples"

echo "=== [hnsw_partition] metrics (full cost_fit model list) ==="
python3 99_print_test_metrics.py

echo "Done. Partition HNSW coef: ${ROOT}/models/hnsw_partition_scan_coef.json"
