# PostgreSQL 17.4 / pgvector 优化器代价公式速查

本文档根据源码归纳 planner 使用的**相对代价单位**（非毫秒），便于查阅。实现位置：

- 核心：`postgresql-17.4/src/backend/optimizer/path/costsize.c`
- 通用索引 I/O/CPU：`postgresql-17.4/src/backend/utils/adt/selfuncs.c` → `genericcostestimate`
- 向量索引：`pgvector/src/ivfflat.c`、`pgvector/src/hnsw.c`

---

## 1. 全局参数（GUC / 表空间可覆盖页代价）

| 符号 | 含义 | 默认值（`cost.h`） |
|------|------|-------------------|
| \(C_\text{seq}\) | `seq_page_cost` | 1.0 |
| \(C_\text{rnd}\) | `random_page_cost` | 4.0 |
| \(C_\text{tuple}\) | `cpu_tuple_cost` | 0.01 |
| \(C_\text{idx}\) | `cpu_index_tuple_cost` | 0.005 |
| \(C_\text{op}\) | `cpu_operator_cost` | 0.0025 |
| \(C_\parallel^\text{tuple}\) | `parallel_tuple_cost` | 0.1 |
| \(C_\parallel^\text{setup}\) | `parallel_setup_cost` | 1000 |
| \(B\) | `effective_cache_size`（页） | 524288 |

**插值与缓存：** 索引/堆访问可能使用 **Mackert–Lohman** `index_pages_fetched(T, pages, …)` 折减重复扫描下的页访问次数（与 \(B\)、`total_table_pages` 等有关）。

**禁用某类路径时** 会额外加上极大的 `disable_cost`（约 \(10^{10}\)）。

**一般约定：** 每个路径有 `startup_cost`（首行之前）与 `total_cost`（全部完成）；部分扫描时代价可线性插值。

---

## 2. 顺序扫描（Seq Scan）+ 过滤 + 投影

记：堆页数 \(P\)，堆元组数 \(N\)，**输出行** \(\hat{R}\)（已过滤后的估计行数）。

- **过滤谓词**（每扫描元组）：\(Q_s^\text{startup}\)、\(Q_s^\text{per\_tuple}\) 来自 `get_restriction_qual_cost`。
- **目标列（投影）**：\(T^\text{startup}\)、\(T^\text{per\_tuple}\) 来自 `path->pathtarget->cost`。

\[
\begin{aligned}
\text{disk\_run} &= C_\text{seq} \cdot P \\
\text{cpu\_run} &= (C_\text{tuple} + Q_s^\text{per\_tuple})\, N \;+\; T^\text{per\_tuple}\, \hat{R} \\
\text{startup} &= Q_s^\text{startup} + T^\text{startup} \\
\text{total} &= \text{startup} + \text{cpu\_run} + \text{disk\_run}
\end{aligned}
\]

**并行：** `cpu_run` 与 \(\hat{R}\) 常按 `parallel_divisor` 缩小；磁盘部分通常不摊薄。

---

## 3. 索引扫描（`cost_index`）——索引 AM + 回表 + qpqual + 投影

AM 先给出：`indexStartupCost`、`indexTotalCost`、选择性 \(s_\text{idx}\)、相关性 \(\rho\)（generic 路径上常为 0）、索引页估计等。

记：堆元组 \(N_\text{heap}\)，回表元组数 \(N_\text{fetch} \approx \text{clamp}(s_\text{idx}\, N_\text{heap})\)；\(c=\rho^2\)。

**堆 I/O（单次扫描、简化写法）：**

- 无相关（\(c=0\)）：\(I_\text{max} \approx N_\text{page}^\text{fetch}\, C_\text{rnd}\)，其中 \(N_\text{page}^\text{fetch}\) 由 Mackert–Lohman 等对 \(N_\text{fetch}\) 折算。
- 完全相关（\(c=1\)）：若访问页数 \(P_f\ge 1\)：\(I_\text{min} = C_\text{rnd} + (P_f-1)\, C_\text{seq}\)。

\[
\text{heap\_IO\_run} \approx I_\text{max} + c\,(I_\text{min} - I_\text{max})
\]

**CPU：** qpqual 每**取堆一行**付 \(C_\text{tuple} + Q_\text{qp}^\text{per\_tuple}\)；投影仍按输出行 \(\hat{R}\)。

\[
\begin{aligned}
\text{startup} &= \text{indexStartup} + Q_\text{qp}^\text{startup} + T^\text{startup} \\
\text{run} &\approx (\text{indexTotal} - \text{indexStartup}) + \text{heap\_IO\_run} \\
&\quad + (C_\text{tuple} + Q_\text{qp}^\text{per\_tuple})\, N_\text{fetch} + T^\text{per\_tuple}\, \hat{R}
\end{aligned}
\]

### 3.1 `genericcostestimate`（多数自定义 AM 的基线）

- 索引扫描触及元组数（每次扫描）：\(N_\text{idx} \approx \text{round}((s \cdot N_\text{heap}) / n_\text{sa})\)（有 ScalarArray 时乘 \(n_\text{sa}\)）。
- 索引页数：\(P_\text{idx} \approx \left\lceil N_\text{idx}\, P_\text{idx}^\text{tot} / N_\text{idx}^\text{tot} \right\rceil\)。

单次扫描 I/O：

\[
\text{indexTotal}_\text{IO} \approx P_\text{idx}\, C_\text{rnd} \quad (\text{多次外相关时用 } \text{index\_pages\_fetched} \text{ 修正总量再分摊})
\]

索引侧 CPU（\(n_q\) 个 qual，\(n_o\) 个 orderby）：

\[
\text{indexTotal} \mathrel{+}= Q_\text{arg} + N_\text{idx}\, n_\text{sa}\,\bigl(C_\text{idx} + C_\text{op}(n_q + n_o)\bigr)
\]

\[
\text{indexStartup} \approx Q_\text{arg}
\]

相关性：`indexCorrelation = 0`（generic 假设）。

---

## 4. 哈希连接（Hash Join）

记号：外表行 \(R_O\)，内表行 \(R_I\)；hash 子句个数 \(k\)；`numbatches` \(>1\) 时分区溢出。

**Initial（节选）：**

\[
\begin{aligned}
\text{startup} &\approx O_\text{startup} + I_\text{total} + R_I\,(k\, C_\text{op} + C_\text{tuple}) \\
\text{run} &\approx O_\text{run} + R_O\, k\, C_\text{op}
\end{aligned}
\]

若 \( \text{numbatches} > 1\)（\(P_I,P_O\) 为内外表页数估计）：

\[
\text{startup} \mathrel{+}= C_\text{seq}\, P_I,\quad
\text{run} \mathrel{+}= C_\text{seq}\,(P_I + 2 P_O)
\]

**Final（内连接，简化）：** `virtualbuckets` = `num_buckets` × `num_batches`；内桶占有率 \(f_\text{bucket}\)（统计）；通过 hashqual 的输出行数 \(J\)。

\[
\text{run} \mathrel{+}= Q_\text{hash}^\text{per\_tuple}\, R_O\, \text{clamp}(R_I f_\text{bucket})\, \tfrac{1}{2}
\]

\[
\text{run} \mathrel{+}= J\,(C_\text{tuple} + Q_\text{qp}^\text{per\_tuple}),\quad
\text{startup/ run } \mathrel{+}= T^\text{startup},\, T^\text{per\_tuple}\,\hat{R}_\text{out}
\]

SEMI/ANTI 与内表单值性有特殊分支（提前结束用 `inner_scan_frac` 等）。

---

## 5. 归并连接（Merge Join）+ 排序（Sort）

### 5.1 子路径与排序

若需显式 Sort：`cost_sort` = `input_cost` + `cost_tuplesort`。

在 `cost_tuplesort` 中，**每次比较**默认：

\[
C_\text{cmp} = C_\text{cmp}^\text{extra} + 2\, C_\text{op}
\]

元组数强制 \(\ge 2\) 以便取对数。

- **全内存快排：** \(\text{sort\_startup} \approx C_\text{cmp}\, N \log_2 N\)，\(\text{sort\_run} = C_\text{op}\, N\)。

- **Top-K 堆（有界）：** \(\text{sort\_startup} \approx C_\text{cmp}\, N \log_2(2K)\)。

- **外排：** 页访问 \(\approx 2\, N_\text{pages}\, \lceil \log_M(\text{runs})\rceil\)，代价：

\[
\mathrel{+}= N_\text{access}\,(0.75\, C_\text{seq} + 0.25\, C_\text{rnd})
\]

且 \(\text{sort\_startup} \mathrel{+}= C_\text{cmp}\, N \log_2 N\)。

### 5.2 Merge Join CPU（`final_cost_mergejoin`，概念）

设 outer skip / inner skip、有效 inner 扫描比例 `rescanratio`、通过 merge 条件的行数 \(J\)，merge 子句代价 \(Q_m\)，其它 join qual \(Q_\text{qp}\)。

比较与 qual 近似按“比较的元组步数”摊销；输出：

\[
\text{run} \mathrel{+}= J\,(C_\text{tuple} + Q_\text{qp}^\text{per\_tuple}) + T^\text{per\_tuple}\,\hat{R}_\text{out}
\]

内层可能 Materialize：用 `mat_inner_cost` 与 `bare_inner_cost` 取较小者（见源码 `rescanratio`）。

---

## 6. 聚合（Agg）

输入 \(\,N\) 行，分组列数 \(g\)，组数 \(G\)；`transCost` / `finalCost` 含 startup 与 per_tuple（transition 按输入行，finalize 按输出行）。

- **PLAIN：** \(\text{startup} \approx C_\text{in}^\text{total} + \text{trans}(N) + \text{final}(1)\)，\(\text{total} = \text{startup} + C_\text{tuple}\)。

- **SORTED：** \(\text{startup}=C_\text{in}^\text{startup}\)，\(\text{total}=C_\text{in}^\text{total}+\text{trans}(N)+g\,C_\text{op}\,N+\text{final}(G)+C_\text{tuple}\,G\)。

- **HASHED：** \(\text{startup}\approx C_\text{in}^\text{total}+\text{trans}(N)+g\,C_\text{op}\,N+\text{finalStartup}\)，\(\text{total}\) 再 \(+\text{final}(G)+C_\text{tuple}\,G\)。

**溢出磁盘（HASHED/MIXED）：** 深度 `depth` 由 batch 数递归估得；页 \(P \propto N\,\text{width}\)：

\[
\text{startup/ total } \mathrel{+}= P_\text{w}\, C_\text{rnd}\,(\times 2\text{ 惩罚}),\quad
\text{total } \mathrel{+}= P_\text{r}\, C_\text{seq}\,(\times 2)
\]

\[
\text{CPU spill} \approx \text{depth}\, N\, 2\, C_\text{tuple}
\]

**HAVING：** 再对输出乘选择性并加 qual 代价。

---

## 7. pgvector：IVFFlat 索引代价（在 `genericcostestimate` 之后）

若无 `ORDER BY` / `indexorderbys`，代价为 \(\infty\)（优化器不用该索引路径）。

- \(L\)：索引 `lists`（元页）
- \(p\)：GUC `ivfflat_probes`
- \(r = \min(p/L,\, 1)\)
- `sequentialRatio` \(= 0.5\)（源码常量）

\[
\text{indexTotal}' = \text{indexTotal} - 0.5\, P_\text{idx}\,(C_\text{rnd} - C_\text{seq})
\]

\[
\text{indexStartup} \approx \text{indexTotal}' \cdot r
\]

随后在 `startupPages` 与堆 `rel->pages` 关系下可对 startup 再减随机/顺序价差（TOAST 修正，见 `ivfflatcostestimate`）。

**选择性等仍来自 generic**（`indexSelectivity`、`indexTotal` 基准）。

---

## 8. pgvector：HNSW 索引代价（在 `genericcostestimate` 之后）

若无 `indexorderbys`，代价为 \(\infty\)。

从元页读 \(m\)，GUC `hnsw_ef_search` 记 \(e\)；索引元组数 \(N_\text{idx}\)（`path->indexinfo->tuples`）。

\[
\begin{aligned}
\text{entryLevel} &= \left\lfloor \ln N_\text{idx} \cdot M_L(m) \right\rfloor \\
M_0^\text{max} &= M_\text{layer}(m,0)\cdot e \\
\sigma &= 0.55 \cdot \frac{\ln N_\text{idx}}{\ln m \cdot (1 + \ln e)} \\
\text{tuples\_ratio} &= \min\left(1,\, \frac{\text{entryLevel}\cdot m + M_0^\text{max}\cdot \sigma}{N_\text{idx}}\right)
\end{aligned}
\]

（\(M_L\)、\(M_\text{layer}\) 为源码 `HnswGetMl` / `HnswGetLayerM`。）

\[
\text{indexStartup} \approx \text{indexTotal} \cdot \text{tuples\_ratio}
\]

再按 `startupPages` 与堆页情况把部分页从随机访问改为顺序访问并减掉“多估的堆页”开销（见 `hnswcostestimate`）。

---

## 9. 实用说明

1. 所有公式均为**启发式**，与真实时间不成比例；调 `seq_page_cost` / `random_page_cost` / `cpu_*` 会改变计划选择。
2. 向量 **`ORDER BY … <->`** 会走 `indexorderbys`；无排序子句时 IVFFlat/HNSW  planner 代价为无穷大。
3. 详细分支（SEMI JOIN、并行、MARK/RESTORE）以源码为准；上表覆盖最常用的代价骨架。

---

*生成说明：公式与 PostgreSQL 17.4 `costsize.c`、`selfuncs.c` 及 pgvector 当前 tree 中 `ivfflat.c` / `hnsw.c` 对齐；升级主版本后请对照官方源码复核。*
