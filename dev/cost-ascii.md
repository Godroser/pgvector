# PostgreSQL 17.4 / pgvector optimizer cost formulas (ASCII)

Plain-ASCII reference. Costs are arbitrary planner units, not milliseconds.

Source locations:

- Core: postgresql-17.4/src/backend/optimizer/path/costsize.c
- Generic index I/O/CPU: postgresql-17.4/src/backend/utils/adt/selfuncs.c (genericcostestimate)
- Vector indexes: pgvector/src/ivfflat.c, pgvector/src/hnsw.c

------------------------------------------------------------------------------

## 1. Global parameters (GUC; tablespace can override page costs)

Name in prose          GUC variable                 Default (cost.h)
--------------------   --------------------------   ----------------
C_seq                  seq_page_cost                1.0
C_rnd                  random_page_cost              4.0
C_tuple                cpu_tuple_cost                0.01
C_idx                  cpu_index_tuple_cost          0.005
C_op                   cpu_operator_cost             0.0025
C_ptuple               parallel_tuple_cost           0.1
C_psetup               parallel_setup_cost           1000.0
B_pages                effective_cache_size (pages)  524288

Index/heap repeated access may use Mackert-Lohman index_pages_fetched(),
which depends on B_pages, total_table_pages, etc.

If a path type is disabled via enable_* flags, disable_cost (~1e10) is added.

Each path has startup_cost (before first row) and total_cost (all rows).
Partial fetch cost can be interpolated linearly between them.

------------------------------------------------------------------------------

## 2. Seq Scan + filter + projection

Let:
  P        = baserel->pages (heap pages)
  N        = baserel->tuples (heap tuples scanned)
  R_out    = estimated output rows after filter
  Qs_su    = restriction qual startup (get_restriction_qual_cost)
  Qs_pt    = restriction qual per_tuple
  T_su     = pathtarget->cost.startup
  T_pt     = pathtarget->cost.per_tuple

disk_run = C_seq * P
cpu_run  = (C_tuple + Qs_pt) * N  +  T_pt * R_out
startup  = Qs_su + T_su
total    = startup + cpu_run + disk_run

Parallel: cpu_run and R_out often divided by parallel_divisor; disk_run usually not.

------------------------------------------------------------------------------

## 3. Index Scan (cost_index): AM + heap fetch + qpqual + projection

AM returns: indexStartupCost, indexTotalCost, indexSelectivity s_idx,
indexCorrelation rho (often 0 for generic AM), index page stats.

Let:
  N_heap   = baserel->tuples
  N_fetch  ~ clamp(s_idx * N_heap)
  c        = rho^2

Heap I/O run cost (single-scan sketch):

  If perfectly uncorrelated (c=0): I_max ~ N_page_fetch * C_rnd
    N_page_fetch from Mackert-Lohman on N_fetch, etc.
  If perfectly correlated (c=1) and P_f heap pages touched, P_f>=1:
    I_min = C_rnd + (P_f - 1) * C_seq

heap_IO_run ~ I_max + c * (I_min - I_max)

CPU: for each heap tuple fetched, C_tuple + Qqp_pt; projection uses R_out.

startup ~ indexStartup + Qqp_su + T_su
run     ~ (indexTotal - indexStartup) + heap_IO_run
        + (C_tuple + Qqp_pt) * N_fetch + T_pt * R_out

### 3.1 genericcostestimate baseline

Let:
  s         = combined index selectivity (index quals + partial predicate)
  N_sa      = ScalarArray-induced internal scan count (>=1)
  N_outer   = loop_count
  N_scan    = N_sa * N_outer
  N_idx_tot = total index tuples
  P_idx_tot = total index pages
  P_nl      = non-leaf index pages

Per-scan visited index tuples:
  N_idx ~ clamp( round( (s * N_heap) / N_sa ), [1, N_idx_tot] )

Per-scan touched index pages:
  P_idx ~ max(1, ceil( N_idx * (P_idx_tot - P_nl) / N_idx_tot ))

I/O part:
  if N_scan == 1:
    C_io = P_idx * C_rnd
  else:
    pages_fetched = index_pages_fetched(P_idx * N_scan, P_idx_tot, P_idx_tot, root)
    C_io = (pages_fetched * C_rnd) / N_outer

CPU part (n_q = #indexquals, n_o = #index order by expressions):
  Q_arg = index_other_operands_eval_cost(indexQuals)
        + index_other_operands_eval_cost(indexOrderBys)
  Q_op  = C_op * (n_q + n_o)
  C_cpu = Q_arg + N_idx * N_sa * (C_idx + Q_op)

So:
  indexStartup = Q_arg
  indexTotal   = C_io + C_cpu
  indexCorrelation = 0 (generic assumption)

------------------------------------------------------------------------------

## 4. Hash Join

R_O = outer rows, R_I = inner rows, k = number of hash clauses.
P_I, P_O = estimated inner/outer relation pages (for spill).

initial_cost_hashjoin (core):

  startup ~ O_startup + I_total + R_I * (k * C_op + C_tuple)
  run     ~ O_run + R_O * k * C_op

If numbatches > 1:

  startup += C_seq * P_I
  run     += C_seq * (P_I + 2 * P_O)

final_cost_hashjoin (inner join sketch):

  f_bucket = inner bucket occupancy (stats; min over hash clauses)
  J        = rows passing hashquals (approx_tuple_count)

  run += Q_hash_per_tuple * R_O * clamp(R_I * f_bucket) * 0.5

  run += J * (C_tuple + Qqp_per_tuple)
  startup/run += T_su + T_pt * R_out

SEMI/ANTI and inner_unique use inner_scan_frac and different tuple estimates.

------------------------------------------------------------------------------

## 5. Merge Join + Sort

If explicit Sort: cost_sort = input_cost + cost_tuplesort tuplesort part.

Per comparison in cost_tuplesort (default):

  C_cmp = C_cmp_extra + 2 * C_op

Input tuple count is forced to at least 2 before log.

In-memory quicksort-style:

  sort_startup ~ C_cmp * N * log2(N)
  sort_run     = C_op * N

Top-K heap (bounded):

  sort_startup ~ C_cmp * N * log2(2*K)

External sort:

  npageaccess ~ 2 * N_pages * ceil( log(runs) / log(mergeorder) )
  add: npageaccess * (0.75 * C_seq + 0.25 * C_rnd)
  sort_startup also += C_cmp * N * log2(N)

final_cost_mergejoin output CPU (concept):

  run += J * (C_tuple + Qqp_per_tuple) + T_pt * R_out

J = rows passing mergequals; merge qual CPU charged on skip/comparison steps
with rescanratio on inner side. May choose Materialize inner (mat_inner_cost
vs bare_inner_cost).

------------------------------------------------------------------------------

## 6. Aggregate (Agg)

N     = input rows
g     = numGroupCols
G     = numGroups
trans(N), final(G) = summed startup + per-tuple costs from AggClauseCosts

PLAIN:
  startup ~ input_total + trans(N) + final(1)
  total   = startup + C_tuple

SORTED:
  startup = input_startup
  total   = input_total + trans(N) + g*C_op*N + final(G) + C_tuple*G

HASHED:
  startup ~ input_total + trans(N) + g*C_op*N + final_startup
  total   = startup + final(G) + C_tuple*G

Spill to disk (HASHED/MIXED): depth from batches/partitions; pages ~ f(N,width)

  startup/total += P_w * C_rnd * (x2 penalty factor in code)
  total           += P_r * C_seq * (x2)
  CPU spill       ~ depth * N * 2 * C_tuple

HAVING quals: add qual cost and multiply output rows by selectivity.

------------------------------------------------------------------------------

## 7. pgvector IVFFlat (after genericcostestimate)

Without indexorderbys: index costs = infinity (path disabled).

L     = lists (from metapage)
p     = ivfflat_probes GUC
r     = min(p / L, 1.0)
seq_ratio = 0.5  (hardcoded in ivfflat.c)
P_idx = costs.numIndexPages
P_heap = rel->pages

  indexTotal' =
    indexTotal_gen - seq_ratio * P_idx * (C_rnd - C_seq)

  indexStartup0 = indexTotal' * r
  startupPages  = P_idx * r

If startupPages > P_heap and r < 0.5:
  indexStartup =
    indexStartup0
    - (1 - seq_ratio) * startupPages * (C_rnd - C_seq)
    - (startupPages - P_heap) * C_seq
else:
  indexStartup = indexStartup0

indexSelectivity/indexCorrelation/indexPages stay from generic path.

------------------------------------------------------------------------------

## 8. pgvector HNSW (after genericcostestimate)

Without indexorderbys: index costs = infinity.

m     = HNSW m from metapage
e     = hnsw_ef_search GUC
N_idx = path->indexinfo->tuples (index tuple count)

  entryLevel    = (int)( log(N_idx) * HnswGetMl(m) )   /* C cast truncates toward zero */
  M0_max        = HnswGetLayerM(m, 0) * e
  sigma         = 0.55 * log(N_idx) / ( log(m) * (1 + log(e)) )
  tuples_ratio  = min( 1.0, (entryLevel * m + M0_max * sigma) / N_idx )

  indexStartup ~ indexTotal * tuples_ratio

Then same style of startup page cost adjustment vs heap pages as IVFFlat.

------------------------------------------------------------------------------

## 9. Notes

1. All formulas are heuristics; tuning C_seq, C_rnd, C_tuple, C_op changes plans.

2. Vector ORDER BY distance uses indexorderbys; without it IVFFlat/HNSW costs
   are infinite in estimation.

3. For SEMI joins, parallelism, mark/restore, see costsize.c in full.

------------------------------------------------------------------------------

Derived from PostgreSQL 17.4 costsize.c + selfuncs.c and pgvector ivfflat.c /
hnsw.c; re-verify after major upgrades.
