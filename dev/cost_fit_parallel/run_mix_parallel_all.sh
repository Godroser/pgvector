#!/usr/bin/env bash
# Mixed concurrent SQL batches: collect -> fit -> optional predict smoke test.
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
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

echo "=== collect mixed batches ==="
# Exclude ivf_scan if your DB has no IVFFlat data / samples jsonl.
OPS="${MIX_OPS:-sort,mergejoin,hashjoin,scan,index_scan,agg}"
python3 -u 01_collect_mix_parallel.py \
  --batches "${MIX_BATCHES:-300}" \
  --degree-min "${MIX_DMIN:-2}" \
  --degree-max "${MIX_DMAX:-5}" \
  --target-per-op "${MIX_TARGET_PER_OP:-60}" \
  --ops "${OPS}" \
  --measure-solo-missing

echo "=== fit mixed model ==="
python3 -u 02_fit_mix_parallel.py \
  --data "${DIR}/data/mix_parallel_samples.jsonl" \
  --out "${DIR}/models/mix_parallel_coef.json" \
  --train-fraction "${MIX_TRAIN_FRAC:-0.8}"

echo "=== predict smoke ==="
echo '{"operators_in_batch":["sort","hashjoin"],"solo_ms":[2.0,50.0],"focus_index":1}' \
  | python3 -u 03_predict_mix_parallel.py --coef "${DIR}/models/mix_parallel_coef.json"

echo "Done. Coef: ${DIR}/models/mix_parallel_coef.json"
