-- HNSW only on three partitions per table (idx 16, 49, 82). Keeps build cost bounded.
-- Prerequisite: 00_create_partitioned_tables.sql

SET maintenance_work_mem = '2GB';

DO $$
DECLARE
  r RECORD;
BEGIN
  FOR r IN
    SELECT child_rel FROM _cost_fit_hnsw_part_bounds WHERE idx IN (16, 49, 82) ORDER BY idx
  LOOP
    EXECUTE format(
      'DROP INDEX IF EXISTS idx_hnsw_%I;',
      r.child_rel
    );
    EXECUTE format(
      'CREATE INDEX idx_hnsw_%I ON %I USING hnsw (text_embedding vector_l2_ops);',
      r.child_rel, r.child_rel
    );
  END LOOP;

  FOR r IN
    SELECT child_rel FROM _cost_fit_hnsw_ps_bounds WHERE idx IN (16, 49, 82) ORDER BY idx
  LOOP
    EXECUTE format(
      'DROP INDEX IF EXISTS idx_hnsw_%I;',
      r.child_rel
    );
    EXECUTE format(
      'CREATE INDEX idx_hnsw_%I ON %I USING hnsw (ps_text_embedding vector_l2_ops);',
      r.child_rel, r.child_rel
    );
  END LOOP;
END $$;

ANALYZE part_vec_p;
ANALYZE partsupp_vec_p;
