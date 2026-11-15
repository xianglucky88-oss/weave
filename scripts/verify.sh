#!/bin/sh
# verify.sh —— 断言 DAG 执行结果。
set -eu
cd "$(dirname "$0")/.."

rm -rf /tmp/weave
python3 -m weave run orders_daily --ds 2026-11-14 > /tmp/weave_ok.json
python3 -m weave run broken_daily --ds 2026-11-14 > /tmp/weave_bad.json || true

python3 - <<'PY'
import json, sqlite3, sys

ok = json.load(open("/tmp/weave_ok.json"))
bad = json.load(open("/tmp/weave_bad.json"))

checks = []
def check(n, cond, d): checks.append((n, cond, d))

check("正常 DAG 全部成功", all(v == "success" for v in ok["statuses"].values()), str(ok["statuses"]))
check("失败 DAG 上游失败", bad["statuses"].get("extract") == "failed", str(bad["statuses"]))
check("失败 DAG 下游被跳过", bad["statuses"].get("transform") == "skipped", str(bad["statuses"]))

conn = sqlite3.connect("/tmp/weave/weave.db")
runs = conn.execute("SELECT count(*) FROM runs").fetchone()[0]
tasks = conn.execute("SELECT count(*) FROM task_runs").fetchone()[0]
check("运行记录已落库", runs >= 2, f"runs={runs}")
check("任务记录已落库", tasks >= 5, f"task_runs={tasks}")

import os
final = "/tmp/weave/data/final/2026-11-14.json"
check("产物已生成", os.path.exists(final), final)

failed = 0
print("== 断言 ==")
for n, c, d in checks:
    if not c: failed += 1
    print(f"  [{'PASS' if c else 'FAIL'}] {n}: {d}")
print(f"\n结果: {len(checks)-failed}/{len(checks)} 通过")
sys.exit(0 if failed == 0 else 1)
PY
