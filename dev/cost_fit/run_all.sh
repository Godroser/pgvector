#!/usr/bin/env bash
# Offline cost fitting pipeline (TPCH10 schema). Uses same env as import_data.sh.
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

echo "=== Optional: indexes for index-scan workloads ==="
"$PSQL" -d "$PGDATABASE" -v ON_ERROR_STOP=1 -f "${DIR}/prepare_indexes.sql" || true

echo "=== Python deps ==="
pip install -q -r "${DIR}/requirements.txt"

echo "=== Collect + fit ==="
cd "$DIR"
python3 01_collect_scan.py
python3 02_fit_scan.py || echo "skip scan fit (need >=5 samples in data/scan_samples.jsonl)"
python3 03_collect_index_scan.py && python3 04_fit_index_scan.py || echo "skip index fit (need indexes/data)"
python3 05_collect_sort.py
python3 06_fit_sort.py || echo "skip sort fit (need >=50 sort samples: 40 train + 10 test)"
python3 07_collect_hashjoin.py
python3 08_fit_hashjoin.py || echo "skip hashjoin fit (need >=4 hash-join samples; planner may choose nest loop)"
python3 09_collect_mergejoin.py && python3 10_fit_mergejoin.py || echo "skip mergejoin fit (no merge plans)"
python3 11_collect_agg.py
python3 12_fit_agg.py || echo "skip agg fit (need >=4 agg samples)"

echo "=== Test metrics (40 train / 10 test by default) ==="
python3 99_print_test_metrics.py

echo "Done. Coefficients under ${DIR}/models/"
