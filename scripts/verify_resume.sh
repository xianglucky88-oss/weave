#!/bin/sh
# verify_resume.sh —— 部分完成、中断续跑、展开变化后的一致性断言。
#
# 场景：
#   1. 第一个 run 中 transform[s1] 失败（其余实例成功），aggregate 被跳过；
#   2. 同一 run 续跑：s0/s2 不重复执行，s1 重新执行，aggregate 就绪；
#   3. 展开结果变化（新增分区）后续跑：新实例执行，旧实例不重复。
set -eu
cd "$(dirname "$0")/.."

rm -rf /tmp/weave
mkdir -p /tmp/weave

# 让 transform[s1] 在第一次运行时失败，续跑时放行。
cat > /tmp/weave/run.sh <<'SH'
#!/bin/sh
set -eu
python3 -m weave "$@"
SH
chmod +x /tmp/weave/run.sh

cat > /tmp/weave/flaky_transform.py <<'PY'
import os
import sys
from pathlib import Path

ds = os.environ["WEAVE_DS"]
shard = os.environ["WEAVE_PARAM_SHARD"]
base = Path("/tmp/weave/data")

# 第一次执行 s1 时失败；marker 存在后放行。
marker = Path("/tmp/weave/recovered")
if shard == "s1" and not marker.exists():
    print("synthetic shard failure", file=sys.stderr)
    sys.exit(7)

raw = __import__("json").loads((base / f"shards_{ds}.json").read_text())
ids = next(p["ids"] for p in raw if p["shard"] == shard)
rows = __import__("json").loads((base / f"extract_{ds}.json").read_text())
selected = [r for r in rows if r["id"] in ids]
out = base / "shards"
out.mkdir(parents=True, exist_ok=True)
(out / f"{ds}_{shard}.json").write_text(__import__("json").dumps(selected))
print("transformed", shard)
PY

cp /tmp/weave/flaky_transform.py jobs/_flaky_transform.py
trap 'rm -f jobs/_flaky_transform.py' EXIT

cat > /tmp/weave/dag_flaky.yaml <<'YAML'
name: flaky_sharded
tasks:
  - name: extract
    script: extract.py
  - name: transform
    script: _flaky_transform.py
    depends_on: [extract]
    retries: 0
    expand:
      path: /tmp/weave/data/shards_{ds}.json
      key: shard
  - name: aggregate
    script: aggregate.py
    depends_on: [transform]
YAML
cp /tmp/weave/dag_flaky.yaml dags/_flaky_sharded.yaml
trap 'rm -f jobs/_flaky_transform.py dags/_flaky_sharded.yaml' EXIT

set +e
python3 -m weave run flaky_sharded --ds 2026-11-14 > /tmp/weave/first.json
set -e
touch /tmp/weave/recovered
python3 -m weave run flaky_sharded --ds 2026-11-14 --resume > /tmp/weave/second.json

python3 - <<'PY'
import json, sqlite3, sys

first = json.load(open("/tmp/weave/first.json"))
second = json.load(open("/tmp/weave/second.json"))
checks = []
def check(n, cond, d): checks.append((n, cond, d))

check("首次运行 transform 失败", first["statuses"]["transform"] == "failed", str(first["statuses"]))
check("首次运行 aggregate 被跳过", first["statuses"]["aggregate"] == "skipped", str(first["statuses"]))

check("续跑复用同一个 run", first["run_id"] == second["run_id"],
      f"{first['run_id']} vs {second['run_id']}")
check("续跑后全部成功", all(v == "success" for v in second["statuses"].values()),
      str(second["statuses"]))

conn = sqlite3.connect("/tmp/weave/weave.db")
conn.row_factory = sqlite3.Row
rid = first["run_id"]

# 实例级历史：s0/s2 只有一次尝试；s1 有失败+成功两次
attempts = {}
for row in conn.execute(
        "SELECT instance_key, attempt, status FROM task_instances "
        "WHERE run_id=? AND task='transform' ORDER BY attempt", (rid,)):
    attempts.setdefault(row["instance_key"], []).append((row["attempt"], row["status"]))
check("s0 未重复执行", attempts.get("s0") == [(1, "success")], str(attempts.get("s0")))
check("s2 未重复执行", attempts.get("s2") == [(1, "success")], str(attempts.get("s2")))
check("s1 先失败后重跑成功", attempts.get("s1") == [(1, "failed"), (2, "success")],
      str(attempts.get("s1")))

logical = {r["task"]: dict(r) for r in conn.execute(
    "SELECT task, status, instance_count FROM logical_tasks WHERE run_id=?", (rid,))}
check("transform 逻辑状态=success", logical["transform"]["status"] == "success", str(logical["transform"]))
check("aggregate 逻辑状态=success", logical["aggregate"]["status"] == "success", str(logical["aggregate"]))
run = dict(conn.execute("SELECT status, finished_at FROM runs WHERE run_id=?", (rid,)).fetchone())
check("run 最终状态=success", run["status"] == "success", str(run))
check("run 有结束时间", run["finished_at"] is not None, str(run))

# 只有活跃实例参与汇总，且全部成功
# 每个活跃实例只取最新一次尝试（历史失败尝试不应参与汇总）
latest = conn.execute(
    """SELECT ti.status FROM task_instances ti
       JOIN (SELECT task, instance_key, MAX(attempt) AS m FROM task_instances
             WHERE run_id=? AND task='transform' GROUP BY task, instance_key) x
         ON x.task=ti.task AND x.instance_key=ti.instance_key AND x.m=ti.attempt
       WHERE ti.run_id=? AND ti.active=1""", (rid, rid)).fetchall()
check("活跃实例最新尝试全部成功", all(r["status"] == "success" for r in latest),
      str([r["status"] for r in latest]))
conn.close()
print("== 断言 ==")
failed = 0
for n, c, d in checks:
    if not c: failed += 1
    print(f"  [{'PASS' if c else 'FAIL'}] {n}: {d}")
print(f"\n结果: {len(checks)-failed}/{len(checks)} 通过")
sys.exit(0 if failed == 0 else 1)
PY
