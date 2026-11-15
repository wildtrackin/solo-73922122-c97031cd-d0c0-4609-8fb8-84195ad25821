#!/usr/bin/env bash
# 无 Docker 时的本机运行辅助：venv + 本机 PostgreSQL。
# 用法：
#   scripts/local_dev.sh init      # 建库、加载基线 schema 与虚构数据
#   scripts/local_dev.sh run       # 启动 API（默认 http://127.0.0.1:8000）
#   scripts/local_dev.sh demo      # 运行端到端演示（需先 run，另开终端）
set -euo pipefail
cd "$(dirname "$0")/.."

DB_NAME="${DB_NAME:-warehouse}"
DB_USER="${DB_USER:-postgres}"
# unix socket 目录按发行版调整（Debian: /var/run/postgresql；本机 conda PG: /tmp）
SOCKET_DIR="${SOCKET_DIR:-/var/run/postgresql}"
DSN="postgresql:///${DB_NAME}?host=${SOCKET_DIR}&user=${DB_USER}"
ADMIN_DSN="postgresql:///postgres?host=${SOCKET_DIR}&user=${DB_USER}"
PSQL="psql -h ${SOCKET_DIR} -U ${DB_USER} -d ${DB_NAME} -v ON_ERROR_STOP=1 -tA"
export DATABASE_URL="$DSN"

case "${1:-run}" in
  init)
    psql -h "$SOCKET_DIR" -U "$DB_USER" -d postgres -tc \
      "SELECT 1 FROM pg_database WHERE datname='${DB_NAME}'" | grep -q 1 \
      || psql -h "$SOCKET_DIR" -U "$DB_USER" -d postgres -c "CREATE DATABASE ${DB_NAME};"
    $PSQL -f db/init/01_baseline.sql
    echo "基线就绪：$($PSQL -c 'SELECT count(*) FROM samples;') 行虚构样本"
    ;;
  run)
    [ -d .venv ] || python3 -m venv .venv
    . .venv/bin/activate
    pip install -q -r requirements.txt
    export BACKFILL_BATCH_SIZE="${BACKFILL_BATCH_SIZE:-2}"
    export BACKFILL_IDLE_SECONDS="${BACKFILL_IDLE_SECONDS:-0.4}"
    export ALLOW_CRASH_DEMO="${ALLOW_CRASH_DEMO:-1}"
    exec uvicorn app.main:app --host 127.0.0.1 --port "${PORT:-8000}"
    ;;
  demo)
    BASE_URL="${BASE_URL:-http://127.0.0.1:${PORT:-8000}}" \
    PSQL="$PSQL" \
    DATABASE_URL="$DSN" PORT="${PORT:-8000}" \
      bash demo/run_demo.sh
    ;;
  test)
    [ -d .venv ] || python3 -m venv .venv
    . .venv/bin/activate
    pip install -q -r requirements.txt pytest
    PATH="$(dirname "$(command -v psql)"):$PATH" \
    TEST_DATABASE="${TEST_DATABASE:-warehouse_pytest}" \
    ADMIN_DATABASE_URL="$ADMIN_DSN" \
      python -m pytest tests/ -v
    ;;
  *)
    echo "用法: $0 {init|run|demo|test}"; exit 1;;
esac
