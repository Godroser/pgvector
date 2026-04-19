#!/usr/bin/env bash
# Offline cost fitting pipeline (TPCH10 schema). Uses same env as import_data.sh.
# HNSW on large vector tables is NOT run here — use hnsw_partition/run_hnsw_partition_fit.sh.
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export PGHOST="${PGHOST:-127.0.0.1}"
export PGPORT="${PGPORT:-5432}"
export PGUSER="${PGUSER:-dzh}"
export PGDATABASE="${PGDATABASE:-tpch10}"
export PSQL="${PSQL:-/data/dzh/postgresql/bin/psql}"
if [[ ! -x "$PSQL" ]]; then PSQL="$(command -v psql)"; fi

echo "=== ANALYZE tables ==="
"$PSQL" -d "$PGDATABASE" -v ON_ERROR_STOP=1 -f "${DIR}/00_analyze.sql"

# echo "=== Optional: indexes for index-scan workloads ==="
# "$PSQL" -d "$PGDATABASE" -v ON_ERROR_STOP=1 -f "${DIR}/prepare_indexes.sql" || true

echo "=== Python deps ==="
pip install -q -r "${DIR}/requirements.txt"

echo "=== Collect + fit ==="
cd "$DIR"
# python3 01_collect_scan.py
# python3 02_fit_scan.py || echo "skip scan fit (need >=50 scan samples: 40 train + 10 test)"
# python3 03_collect_index_scan_new.py && python3 04_fit_index_scan_new.py || echo "skip index fit (need indexes/data)"
python3 05_collect_sort_new.py
python3 06_fit_sort_new.py || echo "skip sort fit (need >=200 sort samples: 160 train + 40 test)"
# python3 07_collect_hashjoin_new.py
# python3 08_fit_hashjoin_new.py || echo "skip hashjoin fit (need >=50 samples or no hash-join plans)"
python3 09_collect_mergejoin_new.py && python3 10_fit_mergejoin_new.py || echo "skip mergejoin fit (need >=200 samples or no merge plans)"
# python3 11_collect_agg_new.py
# python3 12_fit_agg_new.py || echo "skip agg fit (need >=50 agg samples: 40 train + 10 test)"

# echo "=== Vector: IVFFlat on base part/partsupp (optional; drops HNSW on those cols if present) ==="
# "$PSQL" -d "$PGDATABASE" -v ON_ERROR_STOP=1 -f "${DIR}/prepare_vector_indexes_ivf.sql" || true
# python3 15_collect_ivf_scan.py && python3 16_fit_ivf_scan.py || echo "skip ivf fit (need vector extension/data/index build)"

echo "=== Test metrics ==="
python3 99_print_test_metrics.py

echo "Done. Coefficients under ${DIR}/models/"
