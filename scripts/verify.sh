#!/bin/sh
# verify.sh —— 断言 DAG 执行结果、SLA 结论与查询分页。
set -eu
cd "$(dirname "$0")/.."

rm -rf /tmp/weave
python3 -m weave run orders_daily --ds 2026-11-14 > /tmp/weave_ok.json
python3 -m weave run broken_daily --ds 2026-11-14 > /tmp/weave_bad.json || true

# 用固定时间戳再构造一组确定性记录，覆盖重试、缺失时间戳、零 SLA 等边界。
python3 - <<'SEED'
import sqlite3

conn = sqlite3.connect("/tmp/weave/weave.db")
# sla_run：extract 重试两次各 8s（共 16s > SLA 10s），check 6s > SLA 5s；
# 运行整体成功——违规与失败是两件事。
conn.execute("INSERT INTO runs VALUES ('sla_run','orders_daily','2026-11-13',"
             "'success',1000.0,1022.0)")
conn.execute("INSERT INTO task_runs VALUES ('sla_run','extract',1,'failed',1000.0,1008.0,'boom')")
conn.execute("INSERT INTO task_runs VALUES ('sla_run','extract',2,'success',1008.0,1016.0,NULL)")
conn.execute("INSERT INTO task_runs VALUES ('sla_run','check',1,'success',1016.0,1022.0,NULL)")
# partial_run：任务仍在 running，finished_at 缺失，不能据此误报违规。
conn.execute("INSERT INTO runs VALUES ('partial_run','orders_daily','2026-11-12',"
             "'running',2000.0,NULL)")
conn.execute("INSERT INTO task_runs (run_id,task,attempt,status,started_at,finished_at) "
             "VALUES ('partial_run','extract',1,'running',2000.0,NULL)")
# 额外补 4 条普通运行，凑出可分页的数据集。
for i in range(4):
    conn.execute("INSERT INTO runs VALUES (?,?,?, 'success', ?, ?)",
                 (f"page{i}", "orders_daily", "2026-11-11", 3000.0 + i, 3010.0 + i))
conn.commit()
SEED

python3 - <<'PY'
import json
import sqlite3
import sys

from weave.dag import load
from weave.sla import summarize, violations
from weave.store import Store
from pathlib import Path

ok = json.load(open("/tmp/weave_ok.json"))
bad = json.load(open("/tmp/weave_bad.json"))

checks = []
def check(n, cond, d): checks.append((n, bool(cond), d))

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

# ---- SLA 结论：同一份 SQLite 记录反复计算必须稳定 ----
store = Store("/tmp/weave/weave.db")
dag = load(Path("dags/orders.yaml"))
detail = store.run_detail("sla_run")
s1 = summarize(dag, detail["tasks"])
s2 = summarize(dag, store.run_detail("sla_run")["tasks"])
check("SLA 摘要可复现", s1 == s2, str(s1["violations"]))

names = sorted(v["task"] for v in violations(dag, detail["tasks"]))
check("重试总耗时触发违规", names == ["check", "extract"], str(names))
extract = s1["tasks"]["extract"]
check("重试每次尝试都计入总耗时", extract["busy_seconds"] == 16.0, str(extract))
check("退避等待不计入 SLA 耗时", extract["wall_seconds"] == 16.0, str(extract))
check("违规任务所在运行仍可成功", detail["run"]["status"] == "success" and s1["sla_violated"],
      detail["run"]["status"])

# 缺失时间戳：不允许产生违规，也不允许负耗时。
partial = store.run_detail("partial_run")
ps = summarize(dag, partial["tasks"])
check("缺失时间戳不误报违规", ps["violation_count"] == 0 and ps["wall_seconds"] is None,
      str(ps["tasks"]))

# 零 SLA：未设置 SLA 的任务永远不违规。
zero_dag_tasks = [t for t in dag.tasks if t.sla_seconds <= 0]
check("存在零 SLA 任务用于校验", len(zero_dag_tasks) >= 1, str([t.name for t in zero_dag_tasks]))
check("零 SLA 不产生违规",
      all(v["task"] in ("extract", "check") for v in s1["violations"]),
      str([v["task"] for v in s1["violations"]]))

# ---- 过滤与分页：DAG / 状态 / 时间范围 / limit / offset ----
# 数据集共 8 条运行：2 条真实执行 + sla_run + partial_run + 4 条 page。
check("按 DAG 过滤", store.list_runs(dag="orders_daily")["total"] == 7, "")
check("按状态过滤", store.list_runs(status="failed")["total"] == 1, "")
# >=1500：partial_run(2000)、4 条 page(3000..3003) 与 2 条真实运行，共 7 条。
check("按时间范围过滤", store.list_runs(start_at=1500.0)["total"] == 7, "")
check("时间范围闭区间", store.list_runs(start_at=1000.0, end_at=1000.0)["total"] == 1, "")

p1 = store.list_runs(limit=3, offset=0)
p2 = store.list_runs(limit=3, offset=3)
p3 = store.list_runs(limit=3, offset=6)
ids = [r["run_id"] for r in p1["runs"] + p2["runs"] + p3["runs"]]
check("分页不重不漏", len(ids) == 8 and len(set(ids)) == 8, str(ids))
check("total 不随分页变化", p1["total"] == p2["total"] == 8, "")
over = store.list_runs(limit=3, offset=99)
check("offset 越界返回空但保留 total", over["runs"] == [] and over["total"] == 8, "")

failed = 0
print("== 断言 ==")
for n, c, d in checks:
    if not c: failed += 1
    print(f"  [{'PASS' if c else 'FAIL'}] {n}: {d}")
print(f"\n结果: {len(checks)-failed}/{len(checks)} 通过")
sys.exit(0 if failed == 0 else 1)
PY
