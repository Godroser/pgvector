# 算子代价离线拟合（EXPLAIN ANALYZE）

针对 `dev/load_table_tpch10.sql` 中的 TPCH10 schema，用 **EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)** 读取各计划节点的 **Actual Total Time**，并换算为 **独占时间**（inclusive 减去子节点 inclusive），再对特征做线性最小二乘拟合（`numpy.linalg.lstsq`）。

各算子默认在 **`workloads.py`** 中生成 **不少于 55 条** 不同的 SQL（`--target` 可调）；拟合阶段默认 **随机划分 40 条训练 + 10 条测试**（`--seed` / `--train-n` / `--test-n` 可调），并在 `models/*_coef.json` 中写入 **test RMSE / MAE / R² / MAPE**，在 `models/*_test_predictions.jsonl` 中写入测试集逐条 **actual vs predicted**。汇总打印：`python3 99_print_test_metrics.py`（`run_all.sh` 末尾已调用）。

## 环境变量（与 `import_data.sh` 一致）

- `PGHOST`（默认 127.0.0.1）
- `PGPORT`（默认 5432）
- `PGUSER`（默认 dzh）
- `PGDATABASE`（默认 tpch10）
- `PSQL`（默认 `/data/dzh/postgresql/bin/psql`）
- `PGPASSWORD`（若需要）

## 依赖

```bash
pip install -r requirements.txt
```

## 一键运行

```bash
chmod +x run_all.sh
./run_all.sh
```

## 分步脚本

| 步骤 | 脚本 | 输出 |
|------|------|------|
| 统计信息 | `00_analyze.sql` | — |
| 可选索引 | `prepare_indexes.sql` | — |
| Seq Scan 采样 | `01_collect_scan.py` | `data/scan_samples.jsonl` |
| Seq Scan 拟合 | `02_fit_scan.py` | `models/scan_coef.json` |
| Index Scan 采样 | `03_collect_index_scan.py` | `data/index_scan_samples.jsonl` |
| Index Scan 拟合 | `04_fit_index_scan.py` | `models/index_scan_coef.json` |
| Sort 采样 | `05_collect_sort.py` | `data/sort_samples.jsonl` |
| Sort 拟合 | `06_fit_sort.py` | `models/sort_coef.json` |
| Hash Join 采样 | `07_collect_hashjoin.py` | `data/hashjoin_samples.jsonl` |
| Hash Join 拟合 | `08_fit_hashjoin.py` | `models/hashjoin_coef.json` |
| Merge Join 采样 | `09_collect_mergejoin.py` | `data/mergejoin_samples.jsonl` |
| Merge Join 拟合 | `10_fit_mergejoin.py` | `models/mergejoin_coef.json` |
| Aggregate 采样 | `11_collect_agg.py` | `data/agg_samples.jsonl` |
| Aggregate 拟合 | `12_fit_agg.py` | `models/agg_coef.json` + `agg_test_predictions.jsonl` |
| 汇总测试指标 | `99_print_test_metrics.py` | 终端输出 |
| 扫描基线预测（示例） | `13_residual_with_scan.py` | 打印预测 ms |
| SQL 生成 | `workloads.py` | 被各 `*_collect_*.py` import |

## 独占时间说明

PostgreSQL JSON 里每个节点的 `Actual Total Time` 已包含其子树时间。本仓库用：

`exclusive = node.Actual Total Time - sum(child.Actual Total Time)`

因此 **Hash Join / Agg 等拟合目标已是“本算子自身”时间**，一般无需再手动减子 Scan；若你从整条 SQL 墙钟时间做分解，才需要另减子树（可用 `13_residual_with_scan.py` 结合 `scan_coef` 做粗估）。

## 可调部分

- 会话稳定性：`session_prelude.sql`（并行、JIT、超时）
- 工作负载 SQL：`workloads.py`（`scan_workloads`、`index_scan_workloads` 等）；采集脚本支持 `--target`、`--limit`（调试）
- 特征与模型：当前为线性 OLS；可改为 Ridge/Huber 等（改 `fit_common.lstsq_fit`）

## 向量索引

IVFFlat/HNSW 的 `amcostestimate` 与通用 `cost_index` 较复杂；若需要可仿照 `03_collect_index_scan.py` 增加 `ORDER BY emb <-> '[...]'` 工作负载并单独拟合。
