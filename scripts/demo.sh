#!/bin/sh
# demo.sh —— 跑正常 DAG、失败 DAG 和动态展开 DAG，展示状态传播与实例级状态。
set -eu
cd "$(dirname "$0")/.."

rm -rf /tmp/weave

echo "== 1. 可用 DAG =="
python3 -m weave list

echo
echo "== 2. 执行 orders_daily =="
python3 -m weave run orders_daily --ds 2026-11-14

echo
echo "== 3. 执行 broken_daily（上游失败，失败会递归传播）=="
python3 -m weave run broken_daily --ds 2026-11-14 || true

echo
echo "== 4. 执行 sharded_daily（extract 扇出成 3 个 transform 实例，再 reduce）=="
RUN=$(python3 -m weave run sharded_daily --ds 2026-11-14 | python3 -c 'import json,sys;print(json.load(sys.stdin)["run_id"])')
echo "run_id=$RUN"

echo
echo "== 5. 实例级状态（逻辑任务 vs 实例）=="
python3 -m weave status "$RUN" | python3 scripts/_format_status.py

echo
echo "== 6. 同一 run 续跑：已成功实例不重复执行 =="
python3 -m weave run sharded_daily --ds 2026-11-14 --run-id "$RUN"

echo
echo "== 7. 产物 =="
ls -l /tmp/weave/data/final 2>/dev/null || echo "(无产物)"
