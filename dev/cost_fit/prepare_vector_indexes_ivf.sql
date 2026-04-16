-- pgvector IVFFlat-only phase for cost_fit (drop HNSW so plans use IVF).
-- Run before 15_collect_ivf_scan.py

SET maintenance_work_mem = '2GB';

DROP INDEX IF EXISTS idx_part_text_embedding_hnsw;
DROP INDEX IF EXISTS idx_ps_text_embedding_hnsw;

CREATE INDEX IF NOT EXISTS idx_part_text_embedding_ivf
  ON part USING ivfflat (text_embedding vector_l2_ops) WITH (lists = 500);

CREATE INDEX IF NOT EXISTS idx_ps_text_embedding_ivf
  ON partsupp USING ivfflat (ps_text_embedding vector_l2_ops) WITH (lists = 1000);

ANALYZE part;
ANALYZE partsupp;
