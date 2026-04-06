#!/bin/bash

# TPCH10 数据导入：通过 psql 执行 load_table_tpch10.sql，目标库 tpch10

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQL_FILE="${SCRIPT_DIR}/load_table_tpch10.sql"

# PostgreSQL 连接（可用环境变量覆盖）
export PGHOST="${PGHOST:-127.0.0.1}"
export PGPORT="${PGPORT:-5432}"
export PGUSER="${PGUSER:-dzh}"
PGDATABASE="${PGDATABASE:-tpch10}"

PSQL="${PSQL:-/data/dzh/postgresql/bin/psql}"
if [[ ! -x "$PSQL" ]]; then
  PSQL="$(command -v psql)"
fi

if [[ ! -f "$SQL_FILE" ]]; then
  echo "错误: 找不到 SQL 文件: $SQL_FILE"
  exit 1
fi

echo "=========================================="
echo "TPCH10 导入 PostgreSQL"
echo "  主机: $PGHOST:$PGPORT"
echo "  用户: $PGUSER"
echo "  数据库: $PGDATABASE"
echo "  SQL: $SQL_FILE"
echo "=========================================="

echo ""
echo "步骤 1: 确保数据库 ${PGDATABASE} 存在..."
if ! "$PSQL" -d postgres -v ON_ERROR_STOP=1 -tAc "SELECT 1 FROM pg_database WHERE datname = '${PGDATABASE}'" | grep -qx 1; then
  "$PSQL" -d postgres -v ON_ERROR_STOP=1 -c "CREATE DATABASE \"${PGDATABASE}\";"
  echo "已创建数据库 ${PGDATABASE}"
else
  echo "数据库 ${PGDATABASE} 已存在"
fi

echo ""
echo "步骤 2: 执行 ${SQL_FILE} ..."
"$PSQL" -d "$PGDATABASE" -v ON_ERROR_STOP=1 -f "$SQL_FILE"

echo ""
echo "=========================================="
echo "导入完成"
echo "=========================================="
