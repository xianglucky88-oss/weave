"""存储层：逻辑任务/实例分层与旧库迁移。"""

from __future__ import annotations

import sqlite3

from weave.ready import Status, rollup_instance_status, summarize_instances
from weave.store import Store


def test_legacy_task_runs_table_is_migrated(tmp_path):
    path = tmp_path / "legacy.db"
    conn = sqlite3.connect(path)
    conn.executescript("""
    CREATE TABLE runs (
        run_id TEXT PRIMARY KEY, dag TEXT NOT NULL, ds TEXT NOT NULL,
        status TEXT NOT NULL, started_at REAL NOT NULL, finished_at REAL
    );
    CREATE TABLE task_runs (
        run_id TEXT NOT NULL, task TEXT NOT NULL, attempt INTEGER NOT NULL,
        status TEXT NOT NULL, started_at REAL, finished_at REAL, error TEXT,
        PRIMARY KEY (run_id, task, attempt)
    );
    INSERT INTO runs VALUES ('old', 'd', '2026-01-01', 'success', 1.0, 2.0);
    INSERT INTO task_runs VALUES ('old', 'a', 1, 'success', 1.0, 1.5, NULL);
    INSERT INTO task_runs VALUES ('old', 'b', 1, 'failed', 1.0, 1.5, 'x');
    """)
    conn.commit()
    conn.close()

    store = Store(path)
    detail = store.run_detail("old")
    assert detail["run"]["status"] == "success"
    # 旧记录变成 instance_key='' 的实例
    by_task = {i["task"]: i for i in detail["instances"]}
    assert by_task["a"]["status"] == "success"
    assert by_task["b"]["status"] == "failed"
    assert by_task["a"]["instance_key"] == ""
    # 兼容视图仍可用
    assert store.statuses("old") == {"a": "success", "b": "failed"}

    # 再次打开不会重复迁移
    store2 = Store(path)
    assert len(store2.instance_attempts("old")) == 2

    # 迁移后可以继续在新 run 上写实例
    run_id = store2.create_run("d", "2026-01-02")
    store2.begin_instance(run_id, "a", "k", "a[k]", 1, {}, "digest")
    store2.finish_instance(run_id, "a", "k", 1, "success")
    assert store2.latest_instance(run_id, "a", "k")["status"] == "success"


def test_attempt_numbering_is_per_instance(tmp_path):
    store = Store(tmp_path / "db.sqlite")
    run = store.create_run("d", "ds")
    assert store.next_attempt(run, "m", "a") == 1
    store.begin_instance(run, "m", "a", "m[a]", 1, {}, "d")
    store.finish_instance(run, "m", "a", 1, "failed", error="x")
    assert store.next_attempt(run, "m", "a") == 2
    assert store.next_attempt(run, "m", "b") == 1


def test_latest_instances_uses_newest_attempt_and_active_flag(tmp_path):
    store = Store(tmp_path / "db.sqlite")
    run = store.create_run("d", "ds")
    store.begin_instance(run, "m", "a", "m[a]", 1, {"v": 1}, "d1")
    store.finish_instance(run, "m", "a", 1, "failed")
    store.begin_instance(run, "m", "a", "m[a]", 2, {"v": 1}, "d1")
    store.finish_instance(run, "m", "a", 2, "success")
    store.begin_instance(run, "m", "b", "m[b]", 1, {"v": 2}, "d2")
    store.deactivate_instance(run, "m", "b")

    latest = store.latest_instances(run)
    assert [(x["instance_key"], x["attempt"], x["status"]) for x in latest] == [
        ("a", 2, "success")
    ]
    # 历史仍然完整
    assert len(store.instance_attempts(run)) == 3


def test_rollup_rules():
    def rec(status):
        return {"status": status}

    assert rollup_instance_status([], expanded=True) == Status.SUCCESS
    assert rollup_instance_status([], expanded=True, expected=2) == Status.RUNNING
    assert rollup_instance_status(
        [rec("success"), rec("success")], expanded=True) == Status.SUCCESS
    assert rollup_instance_status(
        [rec("success"), rec("failed")], expanded=True) == Status.FAILED
    assert rollup_instance_status(
        [rec("success"), rec("running")], expanded=True) == Status.RUNNING
    # 记录数少于预期说明历史不完整，不能判定成功
    assert rollup_instance_status(
        [rec("success")], expanded=True, expected=2) == Status.RUNNING
    assert summarize_instances(
        [rec("success"), rec("failed"), rec("running"), rec("pending")]
    ).as_dict() == {"total": 4, "succeeded": 1, "failed": 1,
                    "running": 1, "pending": 1}
