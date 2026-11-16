"""补跑/重跑规划与执行的完整规则测试。

覆盖：UTC 窗口枚举、三种 on_exists 策略 × 各种运行状态、日期范围限定、
窗口顺序、重复提交、崩溃残留接管、计划中途失败恢复。
"""

from __future__ import annotations

import json
import threading
from datetime import datetime, timezone

import pytest

from weave.dag import DAG, Task
from weave.rerun import (
    RerunConfigError,
    execute_plan,
    plan_reruns,
)
from weave.schedule import ScheduleError
from weave.store import RunAlreadyRunning, Store


def ts(y, m, d, hh=0, mm=0, ss=0):
    return datetime(y, m, d, hh, mm, ss, tzinfo=timezone.utc).timestamp()


NOW = ts(2026, 11, 16, 12)


def make_dag(schedule_name="day"):
    return DAG(name="orders", schedule=schedule_name,
               tasks=[Task(name="a", script="a.py")])


def seed_run(store, dag, ds, status, *, age=None, started_at=None):
    """直接写一条历史运行。返回 run_id。"""
    run_id = store.create_run(dag, ds)
    if started_at is None:
        started_at = (NOW - age) if age is not None else NOW - 100
    store.conn.execute("UPDATE runs SET started_at=? WHERE run_id=?",
                       (started_at, run_id))
    if status != "running":
        store.finish_run(run_id, status)
    store.conn.commit()
    return run_id


# ---------------------------------------------------------------- 规划基础


def test_catchup_never_ran_has_no_anchor(tmp_path):
    store = Store(tmp_path / "db.sqlite")
    plan = plan_reruns(make_dag(), store, now=NOW)
    assert plan.mode == "catchup"
    assert plan.decisions == []
    assert any("从未运行" in n for n in plan.notes)


def test_catchup_includes_anchor_window_for_retry(tmp_path):
    # 锚点窗口失败：即使之后没有新窗口，也要把锚点本身重新评估为 run。
    store = Store(tmp_path / "db.sqlite")
    rid = seed_run(store, "orders", "2026-11-15", "failed",
                   started_at=ts(2026, 11, 15, 3))
    plan = plan_reruns(make_dag(), store, now=NOW)
    by_ds = {d.ds: d for d in plan.decisions}
    anchor = by_ds["2026-11-15"]
    assert anchor.action == "run" and anchor.state == "failed"
    assert anchor.run_id == rid
    assert "失败" in anchor.reason


def test_catchup_long_outage_lists_every_missed_day(tmp_path):
    store = Store(tmp_path / "db.sqlite")
    seed_run(store, "orders", "2026-11-10", "success",
             started_at=ts(2026, 11, 10, 2))
    plan = plan_reruns(make_dag(), store, now=ts(2026, 11, 14, 9))
    dss = [d.ds for d in plan.decisions]
    # 锚点 11-10 成功（跳过）+ 11..13 已关闭漏跑（补）；
    # 11-14 是当前开放窗口，单独评估为 skip 并给出原因。
    assert dss == ["2026-11-10", "2026-11-11", "2026-11-12",
                   "2026-11-13", "2026-11-14"]
    assert [d.ds for d in plan.run_windows] == [
        "2026-11-11", "2026-11-12", "2026-11-13"]
    open_decision = next(d for d in plan.decisions if d.ds == "2026-11-14")
    assert open_decision.is_open and open_decision.action == "skip"


def test_open_window_is_evaluated_but_skipped_with_reason(tmp_path):
    store = Store(tmp_path / "db.sqlite")
    seed_run(store, "orders", "2026-11-15", "success",
             started_at=ts(2026, 11, 15, 1))
    now = ts(2026, 11, 16, 12)  # 11-16 是当前开放窗口
    plan = plan_reruns(make_dag(), store, now=now)
    open_decision = next(d for d in plan.decisions if d.ds == "2026-11-16")
    assert open_decision.is_open is True
    assert open_decision.action == "skip"
    assert "尚未关闭" in open_decision.reason


def test_hour_schedule_ds_granularity_keeps_same_day_windows(tmp_path):
    store = Store(tmp_path / "db.sqlite")
    seed_run(store, "orders", "2026-11-16T08", "success",
             started_at=ts(2026, 11, 16, 8))
    plan = plan_reruns(make_dag("hour"), store, now=ts(2026, 11, 16, 11))
    dss = [d.ds for d in plan.decisions]
    assert "2026-11-16T08" in dss
    assert "2026-11-16T09" in dss and "2026-11-16T10" in dss
    # 11 点窗口仍开放：catchup 上界是 now(11:00)，该窗口起点等于上界，
    # 落在 [lo, hi) 之外，因此不出现在计划里（留给实时触发）。
    assert "2026-11-16T11" not in dss


def test_hour_schedule_explicit_range_shows_open_window(tmp_path):
    store = Store(tmp_path / "db.sqlite")
    plan = plan_reruns(
        make_dag("hour"), store,
        start="2026-11-16T08", end="2026-11-16T11",
        now=ts(2026, 11, 16, 11, 30))
    dss = [d.ds for d in plan.decisions]
    # 08/09/10 已关闭，11 点窗口在 11:30 仍开放但被显式范围覆盖。
    assert dss == ["2026-11-16T08", "2026-11-16T09",
                   "2026-11-16T10", "2026-11-16T11"]
    op = next(d for d in plan.decisions if d.ds == "2026-11-16T11")
    assert op.is_open and op.action == "skip"


def test_rerun_explicit_date_range_clamped_to_closed_windows(tmp_path):
    store = Store(tmp_path / "db.sqlite")
    plan = plan_reruns(
        make_dag(), store, start="2026-11-10", end="2026-11-14", now=NOW)
    assert plan.mode == "rerun"
    # end 解析为 11-15T00:00（开区间上界），恰好覆盖 10..14 五个已关闭日期。
    assert [d.ds for d in plan.decisions] == [
        "2026-11-10", "2026-11-11", "2026-11-12", "2026-11-13", "2026-11-14"]
    assert all(d.action == "run" for d in plan.decisions)
    assert all(not d.is_open for d in plan.decisions)


def test_rerun_range_can_include_open_window_explained(tmp_path):
    store = Store(tmp_path / "db.sqlite")
    # end 给到 11-16，上界 11-17T00:00，当前开放的 11-16 窗口被纳入但跳过。
    plan = plan_reruns(
        make_dag(), store, start="2026-11-14", end="2026-11-16", now=NOW)
    assert [d.ds for d in plan.decisions] == [
        "2026-11-14", "2026-11-15", "2026-11-16"]
    assert [d.ds for d in plan.run_windows] == ["2026-11-14", "2026-11-15"]
    op = plan.decisions[-1]
    assert op.is_open and op.action == "skip" and "尚未关闭" in op.reason


def test_rerun_future_end_is_clamped_to_now(tmp_path):
    store = Store(tmp_path / "db.sqlite")
    plan = plan_reruns(
        make_dag(), store, start="2026-11-14", end="2026-12-31", now=NOW)
    assert any("未来" in n for n in plan.notes)
    # 截到 now 后最后一个已关闭窗口是 11-15；开放窗口 11-16 单独 skip。
    assert plan.decisions[-2].ds == "2026-11-15"
    assert plan.decisions[-1].ds == "2026-11-16"
    assert plan.decisions[-1].is_open


def test_rerun_requires_start_when_no_history(tmp_path):
    store = Store(tmp_path / "db.sqlite")
    with pytest.raises(RerunConfigError):
        plan_reruns(make_dag(), store, end="2026-11-14", now=NOW)


def test_invalid_range_and_policy_rejected(tmp_path):
    store = Store(tmp_path / "db.sqlite")
    with pytest.raises(ScheduleError):
        plan_reruns(make_dag(), store, start="bad", end="2026-11-14", now=NOW)
    with pytest.raises(RerunConfigError):
        plan_reruns(make_dag(), store, start="2026-11-14", end="2026-11-10", now=NOW)
    with pytest.raises(RerunConfigError):
        plan_reruns(make_dag(), store, start="2026-11-10", end="2026-11-14",
                    on_exists="bogus", now=NOW)


def test_dag_without_schedule_rejected(tmp_path):
    store = Store(tmp_path / "db.sqlite")
    with pytest.raises(RerunConfigError):
        plan_reruns(make_dag(""), store, now=NOW)


def test_max_windows_truncation(tmp_path):
    store = Store(tmp_path / "db.sqlite")
    seed_run(store, "orders", "2026-11-10", "success",
             started_at=ts(2026, 11, 10))
    # catchup 到 11-13T00:00：锚点 10 + 已关闭的 11、12，共 3 个窗口。
    plan = plan_reruns(make_dag(), store, now=ts(2026, 11, 13),
                       max_windows=2)
    assert plan.truncated is True
    assert len(plan.decisions) == 2
    assert [d.ds for d in plan.decisions] == ["2026-11-10", "2026-11-11"]
    assert any("截断" in n for n in plan.notes)


# ------------------------------------------------------- 策略 × 状态矩阵


def test_policy_skip_keeps_everything(tmp_path):
    store = Store(tmp_path / "db.sqlite")
    seed_run(store, "orders", "2026-11-10", "success",
             started_at=ts(2026, 11, 10))
    seed_run(store, "orders", "2026-11-11", "failed",
             started_at=ts(2026, 11, 11))
    plan = plan_reruns(make_dag(), store, start="2026-11-10", end="2026-11-12",
                       on_exists="skip", now=NOW)
    states = {d.ds: (d.action, d.state) for d in plan.decisions}
    assert states["2026-11-10"] == ("skip", "success")
    assert states["2026-11-11"] == ("skip", "failed")
    assert states["2026-11-12"] == ("run", "never_run")


def test_policy_retry_default_only_reruns_non_success(tmp_path):
    store = Store(tmp_path / "db.sqlite")
    seed_run(store, "orders", "2026-11-10", "success",
             started_at=ts(2026, 11, 10))
    seed_run(store, "orders", "2026-11-11", "failed",
             started_at=ts(2026, 11, 11))
    seed_run(store, "orders", "2026-11-12", "interrupted",
             started_at=ts(2026, 11, 12))
    plan = plan_reruns(make_dag(), store, start="2026-11-10", end="2026-11-13",
                       now=NOW)
    actions = {d.ds: d.action for d in plan.decisions}
    assert actions["2026-11-10"] == "skip"     # 部分窗口已成功
    assert actions["2026-11-11"] == "run"
    assert actions["2026-11-12"] == "run"
    assert actions["2026-11-13"] == "run"


def test_policy_force_regenerates_success_too(tmp_path):
    store = Store(tmp_path / "db.sqlite")
    seed_run(store, "orders", "2026-11-10", "success",
             started_at=ts(2026, 11, 10))
    plan = plan_reruns(make_dag(), store, start="2026-11-10", end="2026-11-10",
                       on_exists="force", now=NOW)
    d = plan.decisions[0]
    assert d.action == "run" and d.state == "success"
    assert "force" in d.reason


def test_locked_window_is_skipped_under_all_policies(tmp_path):
    store = Store(tmp_path / "db.sqlite", stale_after=3600)
    rid = seed_run(store, "orders", "2026-11-10", "running",
                   started_at=NOW - 60)  # 新鲜锁
    for policy in ("skip", "retry", "force"):
        plan = plan_reruns(make_dag(), store, start="2026-11-10",
                           end="2026-11-10", on_exists=policy, now=NOW,
                           stale_after=3600)
        d = plan.decisions[0]
        assert d.action == "skip" and d.state == "locked"
        assert rid in d.reason


def test_stale_running_is_treated_as_crash_residue(tmp_path):
    store = Store(tmp_path / "db.sqlite", stale_after=300)
    rid = seed_run(store, "orders", "2026-11-10", "running",
                   started_at=ts(2026, 11, 10))  # 远早于 NOW，超过 300s
    plan = plan_reruns(make_dag(), store, start="2026-11-10",
                       end="2026-11-10", now=NOW, stale_after=300)
    d = plan.decisions[0]
    assert d.state == "stale" and d.action == "run"
    assert rid in d.reason and "崩溃" in d.reason


def test_plan_dict_is_fully_explained(tmp_path):
    store = Store(tmp_path / "db.sqlite")
    seed_run(store, "orders", "2026-11-10", "success",
             started_at=ts(2026, 11, 10))
    # end 解析为 11-12T00:00：10 成功跳过、11 漏跑，共两个已关闭窗口。
    plan = plan_reruns(make_dag(), store, start="2026-11-10",
                       end="2026-11-11", now=ts(2026, 11, 12))
    payload = plan.to_dict()
    assert payload["summary"] == {
        "windows_evaluated": 2, "to_run": 1, "skipped": 1, "truncated": False}
    w = payload["windows"][0]
    assert {"ds", "state", "action", "reason", "run_id",
            "window_start_iso", "window_end_iso"} <= set(w)
    assert w["reason"]  # 非空解释


# ------------------------------------------------------------- 执行与恢复


class FakeRunner:
    """可编排成功/失败的假执行器，记录调用顺序。"""

    def __init__(self, fail=()):
        self.fail = set(fail)
        self.calls: list[tuple[str, str]] = []

    def __call__(self, dag, store, run_id, ds, jobs_dir, timeout):
        self.calls.append((run_id, ds))
        status = "failed" if ds in self.fail else "success"
        store.set_task(run_id, "a", 1, status,
                       error="boom" if status == "failed" else None)
        store.finish_run(run_id, status)
        return run_id, {"a": status}


def test_execute_runs_windows_in_ascending_order_with_locks(tmp_path):
    store = Store(tmp_path / "db.sqlite")
    plan = plan_reruns(make_dag(), store, start="2026-11-10",
                       end="2026-11-12", now=NOW)
    fake = FakeRunner()
    report = execute_plan(plan, make_dag(), store, tmp_path, runner=fake)
    assert [ds for _, ds in fake.calls] == [
        "2026-11-10", "2026-11-11", "2026-11-12"]
    assert report.summary["succeeded"] == 3
    # 每次执行都落了新的 run 行并终结。
    rows = store.list_runs(limit=10)
    assert all(r["status"] == "success" for r in rows)


def test_execute_skips_windows_that_became_locked(tmp_path):
    store = Store(tmp_path / "db.sqlite")
    plan = plan_reruns(make_dag(), store, start="2026-11-10",
                       end="2026-11-10", now=NOW)
    # 计划生成后、执行前，另一个触发占住窗口。
    holder = store.acquire_run("orders", "2026-11-10")
    fake = FakeRunner()
    report = execute_plan(plan, make_dag(), store, tmp_path, runner=fake)
    assert fake.calls == []  # 没有重复执行
    assert report.entries[0].status == "skipped_locked"
    assert report.entries[0].run_id == holder


def test_execute_stops_early_and_resumes_idempotently(tmp_path):
    store = Store(tmp_path / "db.sqlite")
    plan = plan_reruns(make_dag(), store, start="2026-11-10",
                       end="2026-11-13", now=NOW)
    fake = FakeRunner(fail={"2026-11-11"})
    report = execute_plan(plan, make_dag(), store, tmp_path, runner=fake)
    assert report.stopped_early is True
    assert report.not_executed == ["2026-11-12", "2026-11-13"]
    assert [ds for _, ds in fake.calls] == ["2026-11-10", "2026-11-11"]

    # 修复后重新触发：计划根据 store 现算，10 号成功被跳过、11 号失败重试、
    # 12/13 继续补——全程不重复跑已经成功的窗口。
    fake2 = FakeRunner()
    plan2 = plan_reruns(make_dag(), store, start="2026-11-10",
                        end="2026-11-13", now=NOW)
    report2 = execute_plan(plan2, make_dag(), store, tmp_path, runner=fake2)
    assert [ds for _, ds in fake2.calls] == [
        "2026-11-11", "2026-11-12", "2026-11-13"]
    assert report2.stopped_early is False
    assert all(e.status == "success" for e in report2.entries)


def test_continue_on_fail_processes_every_window(tmp_path):
    store = Store(tmp_path / "db.sqlite")
    plan = plan_reruns(make_dag(), store, start="2026-11-10",
                       end="2026-11-12", now=NOW)
    fake = FakeRunner(fail={"2026-11-11"})
    report = execute_plan(plan, make_dag(), store, tmp_path,
                          runner=fake, continue_on_fail=True)
    assert [ds for _, ds in fake.calls] == [
        "2026-11-10", "2026-11-11", "2026-11-12"]
    assert report.summary["failed"] == 1 and report.summary["succeeded"] == 2
    assert report.stopped_early is False


def test_runner_exception_marks_interrupted_and_stops(tmp_path):
    store = Store(tmp_path / "db.sqlite")
    plan = plan_reruns(make_dag(), store, start="2026-11-10",
                       end="2026-11-11", now=NOW)

    def boom(dag, store, run_id, ds, jobs_dir, timeout):
        raise RuntimeError("worker died")

    report = execute_plan(plan, make_dag(), store, tmp_path, runner=boom)
    assert report.entries[0].status == "interrupted"
    assert report.not_executed == ["2026-11-11"]
    # 锁已释放，下一次规划会把该窗口识别为 interrupted 并重试。
    plan2 = plan_reruns(make_dag(), store, start="2026-11-10",
                        end="2026-11-11", now=NOW)
    assert plan2.decisions[0].state == "interrupted"
    assert plan2.decisions[0].action == "run"


def test_concurrent_acquire_only_one_wins(tmp_path):
    store = Store(tmp_path / "db.sqlite")
    winners = []
    errors = []

    def attempt():
        try:
            winners.append(store.acquire_run("orders", "2026-11-10"))
        except RunAlreadyRunning as exc:
            errors.append(exc.run_id)

    threads = [threading.Thread(target=attempt) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(winners) == 1
    assert len(errors) == 7


def test_real_runner_end_to_end(tmp_path):
    jobs = tmp_path / "jobs"
    jobs.mkdir()
    (jobs / "ok.py").write_text("print('ok')\n", encoding="utf-8")
    dag = DAG(name="e2e", schedule="day",
              tasks=[Task(name="a", script="ok.py")])
    store = Store(tmp_path / "db.sqlite")
    plan = plan_reruns(dag, store, start="2026-11-10", end="2026-11-11", now=NOW)
    report = execute_plan(plan, dag, store, jobs, timeout=30)
    assert report.summary["succeeded"] == 2
    # 再来一次（retry 默认）：全部成功 -> 没有窗口需要跑。
    plan2 = plan_reruns(dag, store, start="2026-11-10", end="2026-11-11", now=NOW)
    assert plan2.run_windows == []
    assert all(d.state == "success" and d.action == "skip"
               for d in plan2.decisions)


def test_legacy_plan_helper_backwards_compatible(tmp_path):
    from weave.rerun import plan, daterange
    assert daterange("2026-11-10", "2026-11-12") == [
        "2026-11-10", "2026-11-11", "2026-11-12"]
    dag = make_dag()
    got = plan(dag, "2026-11-10", "2026-11-12", {"2026-11-11"})
    assert got == ["2026-11-10", "2026-11-12"]


def test_report_to_dict_json_serializable(tmp_path):
    store = Store(tmp_path / "db.sqlite")
    plan = plan_reruns(make_dag(), store, start="2026-11-10",
                       end="2026-11-10", now=NOW)
    report = execute_plan(plan, make_dag(), store, tmp_path, runner=FakeRunner())
    json.dumps({"plan": plan.to_dict(), "execution": report.to_dict()},
               ensure_ascii=False)  # 不抛异常即可


def test_scheduleless_dag_cannot_plan_but_uses_day_ds_for_manual_run(tmp_path):
    from weave.runner import run_dag
    jobs = tmp_path / "jobs"
    jobs.mkdir()
    (jobs / "ok.py").write_text("print('ok')\n", encoding="utf-8")
    dag = DAG(name="manual", schedule="",
              tasks=[Task(name="a", script="ok.py")])
    store = Store(tmp_path / "db.sqlite")
    # 无 schedule 不能做补跑规划。
    with pytest.raises(RerunConfigError):
        plan_reruns(dag, store, now=NOW)
    # 手工 run 仍可用，ds 退回 day 粒度 UTC 日期。
    run_id, statuses = run_dag(dag, store, "2026-11-10", jobs, timeout=30)
    assert statuses == {"a": "success"}
    assert store.run_detail(run_id)["run"]["ds"] == "2026-11-10"
