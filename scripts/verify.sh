#!/bin/sh
# verify.sh —— 断言 DAG 执行结果与补跑/重跑规则。
set -eu
cd "$(dirname "$0")/.."

rm -rf /tmp/weave
python3 -m weave run orders_daily --ds 2026-11-14 > /tmp/weave_ok.json
python3 -m weave run broken_daily --ds 2026-11-14 > /tmp/weave_bad.json || true

python3 - <<'PY'
import json, sqlite3, subprocess, sys

checks = []
def check(n, cond, d): checks.append((n, cond, d))

ok = json.load(open("/tmp/weave_ok.json"))
bad = json.load(open("/tmp/weave_bad.json"))

check("正常 DAG 全部成功", all(v == "success" for v in ok["statuses"].values()), str(ok["statuses"]))
check("失败 DAG 上游失败", bad["statuses"].get("extract") == "failed", str(bad["statuses"]))
check("失败 DAG 下游被跳过", bad["statuses"].get("transform") == "skipped", str(bad["statuses"]))
check("ds 按调度粒度规范化", ok["ds"].startswith("2026-11-14T"), ok["ds"])

conn = sqlite3.connect("/tmp/weave/weave.db")
runs = conn.execute("SELECT count(*) FROM runs").fetchone()[0]
tasks = conn.execute("SELECT count(*) FROM task_runs").fetchone()[0]
check("运行记录已落库", runs >= 2, f"runs={runs}")
check("任务记录已落库", tasks >= 5, f"task_runs={tasks}")

import os
ds_path = ok["ds"]  # hour 调度规范化后的窗口 ds（2026-11-14T00）
final = f"/tmp/weave/data/final/{ds_path}.json"
check("产物按窗口 ds 生成", os.path.exists(final), final)

# --- 补跑/重跑规则：dry-run 计划必须逐窗口解释 ---
dry = subprocess.run(
    ["python3", "-m", "weave", "rerun", "broken_daily",
     "2026-11-14", "2026-11-15", "--on-exists", "retry", "--dry-run"],
    capture_output=True, text=True, check=True)
plan = json.loads(dry.stdout)
windows = plan["windows"]
check("计划逐窗口给出解释", all(w["reason"] for w in windows), str([w["ds"] for w in windows]))
by_ds = {w["ds"]: w for w in windows}
check("失败窗口在 retry 下补跑",
      by_ds["2026-11-14"]["action"] == "run" and by_ds["2026-11-14"]["state"] == "failed",
      str(by_ds["2026-11-14"]))

# skip 策略：失败窗口被排除
skip = subprocess.run(
    ["python3", "-m", "weave", "rerun", "broken_daily",
     "2026-11-14", "2026-11-14", "--on-exists", "skip", "--dry-run"],
    capture_output=True, text=True, check=True)
sw = json.loads(skip.stdout)["windows"][0]
check("skip 策略排除失败窗口", sw["action"] == "skip", sw["reason"])

# 真实重跑失败 DAG：遇错即停，退出码非 0
real = subprocess.run(
    ["python3", "-m", "weave", "rerun", "broken_daily",
     "2026-11-14", "2026-11-15", "--on-exists", "retry"],
    capture_output=True, text=True)
report = json.loads(real.stdout)["execution"]
check("重跑失败返回非零退出码", real.returncode != 0, f"rc={real.returncode}")
check("遇错即停并登记未执行窗口",
      report["stopped_early"] and report["not_executed"] == ["2026-11-15"],
      str(report["not_executed"]))

# 活动锁：先占住一个仍在执行的窗口，再触发同 ds 的 run 必须被拒（exit 3）
import sqlite3 as _sq, time as _time
_c = _sq.connect("/tmp/weave/weave.db")
_c.execute("INSERT INTO runs (run_id, dag, ds, status, started_at) "
           "VALUES ('heldlock01', 'broken_daily', '2026-11-14', 'running', ?)",
           (_time.time(),))
_c.commit(); _c.close()
lock = subprocess.run(
    ["python3", "-m", "weave", "run", "broken_daily", "--ds", "2026-11-14"],
    capture_output=True, text=True)
check("重复提交被活动锁拒绝", lock.returncode == 3, lock.stderr.strip()[:80])

failed = 0
print("== 断言 ==")
for n, c, d in checks:
    if not c: failed += 1
    print(f"  [{'PASS' if c else 'FAIL'}] {n}: {d}")
print(f"\n结果: {len(checks)-failed}/{len(checks)} 通过")
sys.exit(0 if failed == 0 else 1)
PY
