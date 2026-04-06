-- Stabilize plans for benchmarking (adjust as needed)
SET max_parallel_workers_per_gather = 0;
SET parallel_leader_participation = off;
SET statement_timeout = '300s';
SET jit = off;
