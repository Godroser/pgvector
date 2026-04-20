#!/usr/bin/env bash
# Parallel cost sampling + fit (does not run sibling cost_fit/run_all.sh).
# Requires existing cost_fit single-run jsonl for baselines, or pass --measure-single per op.
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# When stdout is redirected (e.g. nohup > log), Python uses block buffering — logs look "stuck".
export PYTHONUNBUFFERED=1
export PGHOST="${PGHOST:-127.0.0.1}"
export PGPORT="${PGPORT:-5432}"
export PGUSER="${PGUSER:-dzh}"
export PGDATABASE="${PGDATABASE:-tpch10}"
export PSQL="${PSQL:-/data/dzh/postgresql/bin/psql}"
if [[ ! -x "$PSQL" ]]; then PSQL="$(command -v psql)"; fi

echo "=== Python deps ==="
pip install -q -r "${DIR}/requirements.txt"

cd "$DIR"

# Default: degrees 2,4,8 — override with EXTRA_COLLECT_FLAGS="--degrees 2,4,8,16 --rounds 2"
EXTRA_COLLECT_FLAGS="${EXTRA_COLLECT_FLAGS:-}"

collect_fit_one() {
  local op="$1"
  echo "=== collect parallel: ${op} ==="
  python3 -u 01_collect_parallel.py --operator "${op}" --measure-single ${EXTRA_COLLECT_FLAGS}
  echo "=== fit parallel: ${op} ==="
  python3 -u 02_fit_parallel.py --operator "${op}" || echo "skip fit ${op} (need enough rows; adjust train/test or collect more)"
}

# Order: lighter / fewer contention surprises first; comment out what you do not need.
# collect_fit_one sort
# collect_fit_one mergejoin

collect_fit_one scan
collect_fit_one index_scan
collect_fit_one agg
collect_fit_one hashjoin
# IVFFlat / pgvector — requires ivf indexes and data (see cost_fit/prepare_vector_indexes_ivf.sql)
collect_fit_one ivf_scan

echo "=== metrics ==="
python3 -u 99_print_parallel_metrics.py

echo "Done. Parallel coefficients under ${DIR}/models/"
