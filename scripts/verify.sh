#!/bin/sh
# verify.sh —— 断言 DAG 执行结果；重点验证动态展开的一致性。
set -eu
cd "$(dirname "$0")/.."

export PYTHONPATH="${PYTHONPATH:-}"
rm -rf /tmp/weave
python3 -m weave run orders_daily --ds 2026-11-14 > /tmp/weave_ok.json
python3 -m weave run broken_daily --ds 2026-11-14 > /tmp/weave_bad.json || true
python3 -m weave run sharded_daily --ds 2026-11-14 > /tmp/weave_sharded.json

python3 - <<'PY'
import json, os, sqlite3, sys

ok = json.load(open("/tmp/weave_ok.json"))
bad = json.load(open("/tmp/weave_bad.json"))
sharded = json.load(open("/tmp/weave_sharded.json"))

checks = []
def check(n, cond, d): checks.append((n, cond, d))

check("正常 DAG 全部成功", all(v == "success" for v in ok["statuses"].values()), str(ok["statuses"]))
check("失败 DAG 上游失败", bad["statuses"].get("extract") == "failed", str(bad["statuses"]))
check("失败 DAG 下游被跳过", bad["statuses"].get("transform") == "skipped", str(bad["statuses"]))
check("失败 DAG 隔代下游也被跳过", bad["statuses"].get("load") == "skipped", str(bad["statuses"]))
check("展开 DAG 全部成功", all(v == "success" for v in sharded["statuses"].values()), str(sharded["statuses"]))

conn = sqlite3.connect("/tmp/weave/weave.db")
conn.row_factory = sqlite3.Row
run_id = sharded["run_id"]

# 1) 实例真的都执行了，且身份/参数快照正确
inst = conn.execute(
    "SELECT instance_key, instance_name, status, params_json FROM task_instances "
    "WHERE run_id=? AND task='transform' AND active=1", (run_id,)).fetchall()
keys = sorted(r["instance_key"] for r in inst)
check("transform 展开为 3 个实例", keys == ["s0", "s1", "s2"], str(keys))
check("实例命名含 key", all(r["instance_name"] == f"transform[{r['instance_key']}]" for r in inst),
      str([r["instance_name"] for r in inst]))
check("实例参数快照保留基础参数与分区字段",
      all(json.loads(r["params_json"]).get("stage") == "transform" for r in inst),
      str([r["params_json"] for r in inst]))
check("实例状态全成功", all(r["status"] == "success" for r in inst), str([r["status"] for r in inst]))

# 2) 逻辑任务与 run 状态和实例一致
logical = {r["task"]: dict(r) for r in conn.execute(
    "SELECT task, status, instance_count FROM logical_tasks WHERE run_id=?", (run_id,))}
check("transform 逻辑状态=success", logical["transform"]["status"] == "success", str(logical["transform"]))
check("transform 记录实例数=3", logical["transform"]["instance_count"] == 3, str(logical["transform"]))
check("reduce 逻辑状态=success（全部实例成功后才就绪）",
      logical["aggregate"]["status"] == "success", str(logical["aggregate"]))
run = conn.execute("SELECT status FROM runs WHERE run_id=?", (run_id,)).fetchone()
check("run 状态=success", run["status"] == "success", str(dict(run)))

# 3) 历史：每个实例一次尝试，没有重复
attempts = conn.execute(
    "SELECT instance_key, count(*) n, max(attempt) m FROM task_instances "
    "WHERE run_id=? AND task='transform' GROUP BY instance_key", (run_id,)).fetchall()
check("每个实例恰好一次尝试", all(r["n"] == 1 and r["m"] == 1 for r in attempts),
      str([dict(r) for r in attempts]))

# 4) reduce 产物完整（5 行），说明下游读到的实例汇总确实完整
final = "/tmp/weave/data/final/2026-11-14.json"
rows = json.load(open(final))
check("reduce 合并了全部分区的 5 行", len(rows) == 5, f"{len(rows)} rows")
conn.close()

failed = 0
print("== 断言 ==")
for n, c, d in checks:
    if not c: failed += 1
    print(f"  [{'PASS' if c else 'FAIL'}] {n}: {d}")
print(f"\n结果: {len(checks)-failed}/{len(checks)} 通过")
sys.exit(0 if failed == 0 else 1)
PY
