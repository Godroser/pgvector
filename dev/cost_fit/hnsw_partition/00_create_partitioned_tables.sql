-- Standalone HNSW cost_fit: range-partition clones of part / partsupp (100 parts each).
-- Target: part_vec_p, partsupp_vec_p.
--
-- Partition bounds (min/max keys) are read from existing public.part and public.partsupp — those
-- tables must already be loaded. Data for the partitioned tables is loaded from the same CSV
-- layout as dev/load_table_tpch10.sql (COPY to temp, INSERT), not INSERT...SELECT from part.
--
-- WARNING: Drops/recreates part_vec_p, partsupp_vec_p, and helper metadata tables.

SET maintenance_work_mem = '20GB';

DROP TABLE IF EXISTS partsupp_vec_p CASCADE;
DROP TABLE IF EXISTS part_vec_p CASCADE;
DROP TABLE IF EXISTS _cost_fit_hnsw_ps_bounds CASCADE;
DROP TABLE IF EXISTS _cost_fit_hnsw_part_bounds CASCADE;

CREATE TABLE part_vec_p (LIKE part INCLUDING DEFAULTS)
  PARTITION BY RANGE (p_partkey);

CREATE TABLE partsupp_vec_p (LIKE partsupp INCLUDING DEFAULTS)
  PARTITION BY RANGE (ps_partkey);

CREATE TABLE _cost_fit_hnsw_part_bounds (
  idx         int PRIMARY KEY,
  p_lo        int NOT NULL,
  p_hi        int NOT NULL,
  child_rel   text NOT NULL
);

CREATE TABLE _cost_fit_hnsw_ps_bounds (
  idx         int PRIMARY KEY,
  ps_lo       int NOT NULL,
  ps_hi       int NOT NULL,
  child_rel   text NOT NULL
);

DO $$
DECLARE
  min_k    int;
  max_k    int;
  n        int := 100;
  step     int;
  i        int;
  lo       int;
  hi_excl  int;
  rname    text;
  p_max_k  int;
  ps_max_k int;
BEGIN
  SELECT min(p_partkey), max(p_partkey) INTO min_k, max_k FROM part;
  IF min_k IS NULL OR max_k IS NULL THEN
    RAISE EXCEPTION 'part is empty; load data first';
  END IF;
  p_max_k := max_k;
  step := (max_k - min_k + 1 + n - 1) / n;

  FOR i IN 0..n - 1 LOOP
    lo := min_k + i * step;
    IF i < n - 1 THEN
      hi_excl := lo + step;
    ELSE
      hi_excl := NULL;
    END IF;
    rname := format('part_vec_p_p%s', lpad(i::text, 3, '0'));
    IF hi_excl IS NOT NULL THEN
      EXECUTE format(
        'CREATE TABLE %I PARTITION OF part_vec_p FOR VALUES FROM (%s) TO (%s);',
        rname, lo, hi_excl
      );
      INSERT INTO _cost_fit_hnsw_part_bounds (idx, p_lo, p_hi, child_rel)
      VALUES (i, lo, hi_excl - 1, rname);
    ELSE
      EXECUTE format(
        'CREATE TABLE %I PARTITION OF part_vec_p FOR VALUES FROM (%s) TO (MAXVALUE);',
        rname, lo
      );
      INSERT INTO _cost_fit_hnsw_part_bounds (idx, p_lo, p_hi, child_rel)
      VALUES (i, lo, p_max_k, rname);
    END IF;
  END LOOP;

  SELECT min(ps_partkey), max(ps_partkey) INTO min_k, max_k FROM partsupp;
  IF min_k IS NULL OR max_k IS NULL THEN
    RAISE EXCEPTION 'partsupp is empty; load data first';
  END IF;
  ps_max_k := max_k;
  step := (max_k - min_k + 1 + n - 1) / n;

  FOR i IN 0..n - 1 LOOP
    lo := min_k + i * step;
    IF i < n - 1 THEN
      hi_excl := lo + step;
    ELSE
      hi_excl := NULL;
    END IF;
    rname := format('partsupp_vec_p_p%s', lpad(i::text, 3, '0'));
    IF hi_excl IS NOT NULL THEN
      EXECUTE format(
        'CREATE TABLE %I PARTITION OF partsupp_vec_p FOR VALUES FROM (%s) TO (%s);',
        rname, lo, hi_excl
      );
      INSERT INTO _cost_fit_hnsw_ps_bounds (idx, ps_lo, ps_hi, child_rel)
      VALUES (i, lo, hi_excl - 1, rname);
    ELSE
      EXECUTE format(
        'CREATE TABLE %I PARTITION OF partsupp_vec_p FOR VALUES FROM (%s) TO (MAXVALUE);',
        rname, lo
      );
      INSERT INTO _cost_fit_hnsw_ps_bounds (idx, ps_lo, ps_hi, child_rel)
      VALUES (i, lo, ps_max_k, rname);
    END IF;
  END LOOP;
END $$;

-- Load part_vec_p from CSV (same pattern as load_table_tpch10.sql).
BEGIN;

DO $$
DECLARE
  i         int := 0;
  file_path text;
BEGIN
  FOR i IN 0..99 LOOP
    file_path := '/data/dzh/pgvector/data/vector_load_tpch10/part_with_vec_' || i || '.csv';
    EXECUTE format(
      '
            CREATE TEMP TABLE temp_part_load (
                c1 INTEGER, c2 VARCHAR, c3 VARCHAR, c4 VARCHAR, c5 VARCHAR,
                c6 INTEGER, c7 VARCHAR, c8 DECIMAL, c9 VARCHAR, c10 vector(768),
                trailing_null TEXT
            ) ON COMMIT DROP;

            COPY temp_part_load FROM %L WITH (FORMAT csv, DELIMITER ''|'', ENCODING ''UTF8'');

            INSERT INTO part_vec_p (p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment, text_embedding)
            SELECT c1, c2, c3, c4, c5, c6, c7, c8, c9, c10 FROM temp_part_load;

            TRUNCATE temp_part_load;
            DROP TABLE temp_part_load;
        ',
      file_path
    );
    IF i % 10 = 0 THEN
      RAISE NOTICE 'cost_fit part_vec_p CSV: % / 99', i;
    END IF;
  END LOOP;
END $$;

COMMIT;

-- Load partsupp_vec_p from CSV (same pattern as load_table_tpch10.sql).
BEGIN;

DO $$
DECLARE
  i         int;
  file_path text;
BEGIN
  FOR i IN 0..399 LOOP
    file_path := '/data/dzh/pgvector/data/vector_load_tpch10/partsupp_with_vec_' || i || '.csv';
    EXECUTE format(
      '
        CREATE TEMP TABLE temp_ps_load (
            c1 INT,
            c2 INT,
            c3 INT,
            c4 DECIMAL,
            c5 TEXT,
            c6 TEXT,
            c7 vector(768),
            c8 TEXT
        ) ON COMMIT DROP;

        COPY temp_ps_load FROM %L WITH (FORMAT csv, DELIMITER ''|'', ENCODING ''UTF8'');

        INSERT INTO partsupp_vec_p (
            ps_partkey,
            ps_suppkey,
            ps_availqty,
            ps_supplycost,
            ps_comment,
            ps_image_embedding,
            ps_text_embedding
        )
        SELECT c1, c2, c3, c4, c5, NULL, c7 FROM temp_ps_load;

        DROP TABLE temp_ps_load;
    ',
      file_path
    );
    IF i % 50 = 0 THEN
      RAISE NOTICE 'cost_fit partsupp_vec_p CSV: % / 399', i;
    END IF;
  END LOOP;
END $$;

COMMIT;

ANALYZE part_vec_p;
ANALYZE partsupp_vec_p;
