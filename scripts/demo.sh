#!/bin/sh
# demo.sh —— 跑一次正常 DAG 和一次失败 DAG，展示状态传播。
set -eu
cd "$(dirname "$0")/.."

rm -rf /tmp/weave

echo "== 1. 可用 DAG =="
python3 -m weave list

echo
echo "== 2. 执行 orders_daily =="
python3 -m weave run orders_daily --ds 2026-11-14

echo
echo "== 3. 执行 broken_daily（上游失败）=="
python3 -m weave run broken_daily --ds 2026-11-14 || true

echo
echo "== 4. 产物 =="
ls -l /tmp/weave/data/final 2>/dev/null || echo "(无产物)"
