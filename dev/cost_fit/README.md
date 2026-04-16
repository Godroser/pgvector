# cost_fit：算子代价离线拟合

在 **TPCH10** 等已加载数据的 schema 上，用 **EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)** 采集各计划节点的 **Actual Total Time**，换算为 **独占时间**（节点 inclusive 减去子节点 inclusive 之和），再对特征做 **线性最小二乘**（`numpy.linalg.lstsq`），默认 **40 条训练 + 10 条测试**（可调）。

---

## 前置条件

- 数据库中已有 TPCH 表与统计信息（本目录的 `00_analyze.sql` 会再跑一遍 `ANALYZE`）。
- 默认连接：`PGHOST` / `PGPORT` / `PGUSER` / `PGDATABASE`（见下节）；库名默认 **`tpch10`**。
- 可执行的 **`psql`**：若命令行里没有 `psql`，`fit_common.resolve_psql()` 会尝试 `PSQL` 环境变量及常见路径（如 `/data/dzh/postgresql/bin/psql`）。

---

## `run_all.sh`：做什么、怎么用

### 作用

按固定顺序执行整条流水线：**ANALYZE → 可选索引 → 安装 Python 依赖 → 各算子采集与拟合 → 打印测试集指标**。  
在 `cost_fit` 目录下工作，结果写入 `data/` 与 `models/`。

### 使用方法

```bash
cd /path/to/cost_fit
chmod +x run_all.sh   # 仅需一次
./run_all.sh
```

或：

```bash
bash /path/to/cost_fit/run_all.sh
```

### 环境变量（与仓库里 `import_data.sh` 等脚本一致）

| 变量 | 默认 | 说明 |
|------|------|------|
| `PGHOST` | `127.0.0.1` | PostgreSQL 主机 |
| `PGPORT` | `5432` | 端口 |
| `PGUSER` | `dzh` | 用户名 |
| `PGDATABASE` | `tpch10` | 数据库名 |
| `PSQL` | `/data/dzh/postgresql/bin/psql`（若不可执行则退回 `command -v psql`） | `psql` 可执行文件路径 |
| `PGPASSWORD` | — | 需要密码认证时设置 |

示例：

```bash
export PGDATABASE=tpch10
export PSQL=/data/dzh/postgresql/bin/psql
./run_all.sh
```

### 输入

| 输入 | 说明 |
|------|------|
| **PostgreSQL 实例** | 可连接且含 TPCH 表 |
| **本目录 SQL** | `00_analyze.sql`（统计信息）、`prepare_indexes.sql`（可选，失败不中断） |
| **`requirements.txt`** | `pip install` 的 Python 依赖 |

脚本内的 **工作负载 SQL** 由 `workloads.py` 生成（无需单独输入文件）。

### 输出

| 输出位置 | 内容 |
|----------|------|
| **`data/*_samples.jsonl`** | 各算子采集到的一条样本一行 JSON（标签、SQL、特征、独占时间等） |
| **`models/*_coef.json`** | 拟合系数、特征名、训练/测试 RMSE·MAE·R²·MAPE 等（不含逐条测试预测列表） |
| **`models/*_test_predictions.jsonl`** | 测试集逐条 **actual vs predicted**（若对应拟合成功） |
| **标准输出** | 各步骤日志；末尾 **`99_print_test_metrics.py`** 汇总各算子 train/test 指标 |

### 失败与跳过

`set -euo pipefail` 下，若某步 **采集** 失败会导致后续未执行；部分 **拟合** 步骤用 `|| echo "skip ..."` 包了一层：样本不足或计划不符合预期时跳过该算子拟合，流水线仍继续，最后 **`99_print_test_metrics.py`** 会对缺失的 `*_coef.json` 显示 `missing` 或旧格式提示。  
各拟合脚本默认需要 **`train_n + test_n`（默认 40+10=50）** 条样本；采集脚本默认目标约 **55** 条 SQL（`--target`），以保证去重、计划类型过滤后仍够 50 条。

---

## 脚本与文件说明

### SQL 与配置

| 文件 | 作用 |
|------|------|
| **`00_analyze.sql`** | 对 TPCH 相关表执行 `ANALYZE`，稳定统计信息。 |
| **`prepare_indexes.sql`** | 创建可选索引（如 `orders(o_orderdate)`、lineitem 等），供 Index Scan / 部分 join 实验；`run_all.sh` 中失败不中断。 |
| **`session_prelude.sql`** | 会话级设置（并行、JIT、超时等）。采集脚本通过 **`explain_analyze_json(..., tx_prefix=...)`** 与 `EXPLAIN` 放在同一事务里生效；单独 `psql` 跑一句 `SET` 不会作用于下一次连接的 `EXPLAIN`。 |

### 公共 Python 模块

| 文件 | 作用 |
|------|------|
| **`fit_common.py`** | `psql` 调用、`EXPLAIN` JSON 解析、计划树遍历、独占时间、`lstsq`、train/test 划分与指标、`resolve_psql()` 等。 |
| **`workloads.py`** | 为 Seq Scan、Index Scan、Sort、Hash Join、Merge Join、Aggregate 生成多组 `(tag, sql)`；采集脚本 `--target` 控制规模。 |

### 流水线脚本（序号）

| 脚本 | 作用 | 默认主要输出 |
|------|------|----------------|
| **`01_collect_scan.py`** | 对 `workloads.scan_workloads` 中 SQL 跑 `EXPLAIN ANALYZE`，收集 **Seq Scan** 独占时间与特征。 | `data/scan_samples.jsonl` |
| **`02_fit_scan.py`** | 读取 scan 样本，OLS 拟合，train/test 评估。 | `models/scan_coef.json`、`models/scan_test_predictions.jsonl` |
| **`03_collect_index_scan.py`** | `SET LOCAL enable_seqscan TO off`，收集 **Index Scan / Index Only Scan**（需索引与计划匹配）。 | `data/index_scan_samples.jsonl` |
| **`04_fit_index_scan.py`** | Index Scan 拟合。 | `models/index_scan_coef.json`、`models/index_scan_test_predictions.jsonl` |
| **`05_collect_sort.py`** | 在同事务内附加关闭索引扫描等 GUC，避免出现「只有索引有序扫描、无 Sort 节点」；收集 **Sort / Incremental Sort**。 | `data/sort_samples.jsonl` |
| **`06_fit_sort.py`** | Sort 拟合。 | `models/sort_coef.json`、`models/sort_test_predictions.jsonl` |
| **`07_collect_hashjoin.py`** | 通过 GUC 倾向 **Hash Join**，采集 Hash Join 节点样本。 | `data/hashjoin_samples.jsonl` |
| **`08_fit_hashjoin.py`** | Hash Join 拟合。 | `models/hashjoin_coef.json`、`models/hashjoin_test_predictions.jsonl` |
| **`09_collect_mergejoin.py`** | 倾向 **Merge Join**，采集样本。 | `data/mergejoin_samples.jsonl` |
| **`10_fit_mergejoin.py`** | Merge Join 拟合。 | `models/mergejoin_coef.json`、`models/mergejoin_test_predictions.jsonl` |
| **`11_collect_agg.py`** | 采集 **Aggregate** 节点样本。 | `data/agg_samples.jsonl` |
| **`12_fit_agg.py`** | Aggregate 拟合。 | `models/agg_coef.json`、`models/agg_test_predictions.jsonl` |
| **`99_print_test_metrics.py`** | 读取 `models/*_coef.json`，在终端打印各算子样本数与 train/test 指标（对缺失或非数值字段安全降级）。 | 仅 stdout |
| **`13_residual_with_scan.py`** | **可选示例**：用已拟合的 `scan_coef.json` + `pg_class` 统计，估算某表 Seq Scan 毫秒级预测；用于对照「全表时间」与独占时间，非流水线必需。 | 打印预测值 |

采集类脚本常见参数：**`--out`**、**`--target`**（工作负载条数）、**`--limit`**（只跑前 N 条调试）、**`--repeats`**（重复执行取中位数）。  
拟合类脚本常见参数：**`--data`**、**`--out`**、**`--train-n`**、**`--test-n`**、**`--seed`**。

### 依赖

```bash
pip install -r requirements.txt
```

---

## 独占时间说明

计划中每个节点的 `Actual Total Time` 包含子树时间。本仓库使用：

`exclusive = node.Actual Total Time − sum(child.Actual Total Time)`

因此 Join / Agg 等拟合目标已是**该算子自身**耗时。若你从整条 SQL 墙钟时间手动分解，才需要额外减子树；可用 **`13_residual_with_scan.py`** 与 scan 系数做粗估对照。

---

## 向量索引（扩展方向）

IVFFlat/HNSW 的 `amcostestimate` 与通用 `cost_index` 较复杂；若需要可仿照 **`03_collect_index_scan.py`** 增加 `ORDER BY emb <-> '[...]'` 等工作负载并单独拟合。
