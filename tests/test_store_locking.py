"""活动锁、重复提交防护与崩溃残留接管的存储层测试。"""

from __future__ import annotations

import time

import pytest

from weave.store import DEFAULT_STALE_AFTER, RunAlreadyRunning, Store


def test_acquire_creates_and_second_acquire_is_blocked(tmp_path):
    s = Store(tmp_path / "db.sqlite")
    rid = s.acquire_run("dag", "2026-11-14")
    assert rid
    with pytest.raises(RunAlreadyRunning) as ei:
        s.acquire_run("dag", "2026-11-14")
    assert ei.value.run_id == rid


def test_locks_are_scoped_per_dag_and_ds(tmp_path):
    s = Store(tmp_path / "db.sqlite")
    s.acquire_run("dag", "2026-11-14")
    # 同 DAG 不同 ds、不同 DAG 同 ds 都不冲突。
    assert s.acquire_run("dag", "2026-11-15")
    assert s.acquire_run("other", "2026-11-14")


def test_finished_run_releases_lock_for_new_acquire(tmp_path):
    s = Store(tmp_path / "db.sqlite")
    rid1 = s.acquire_run("dag", "2026-11-14")
    s.finish_run(rid1, "success")
    rid2 = s.acquire_run("dag", "2026-11-14")
    assert rid2 != rid1


def test_stale_running_is_taken_over(tmp_path):
    s = Store(tmp_path / "db.sqlite", stale_after=60)
    old = s.acquire_run("dag", "2026-11-14")
    # 把旧运行回拨成 2 分钟前启动，模拟进程崩溃。
    s.conn.execute("UPDATE runs SET started_at=? WHERE run_id=?",
                   (time.time() - 120, old))
    s.conn.commit()
    new = s.acquire_run("dag", "2026-11-14")
    assert new != old
    detail_old = s.run_detail(old)["run"]
    detail_new = s.run_detail(new)["run"]
    assert detail_old["status"] == "interrupted"
    assert detail_new["status"] == "running"


def test_reopen_store_marks_stale_runs_interrupted(tmp_path):
    db = tmp_path / "db.sqlite"
    s1 = Store(db, stale_after=60)
    rid = s1.acquire_run("dag", "2026-11-14")
    s1.conn.execute("UPDATE runs SET started_at=? WHERE run_id=?",
                    (time.time() - 120, rid))
    s1.conn.commit()
    # 模拟服务重启：重新打开同一个库。
    s2 = Store(db, stale_after=60)
    assert s2.run_detail(rid)["run"]["status"] == "interrupted"
    # 锁索引已建，新的 acquire 正常。
    assert s2.acquire_run("dag", "2026-11-14")


def test_latest_run_per_ds_picks_newest(tmp_path):
    s = Store(tmp_path / "db.sqlite")
    r1 = s.acquire_run("dag", "2026-11-14")
    s.finish_run(r1, "failed")
    r2 = s.acquire_run("dag", "2026-11-14")
    s.finish_run(r2, "success")
    s.acquire_run("dag", "2026-11-15")  # running，另一个 ds
    latest = s.latest_run_per_ds("dag", ["2026-11-14", "2026-11-15", "2026-11-16"])
    assert latest["2026-11-14"]["run_id"] == r2
    assert latest["2026-11-14"]["status"] == "success"
    assert latest["2026-11-15"]["status"] == "running"
    assert "2026-11-16" not in latest


def test_list_runs_offset_now_applied(tmp_path):
    s = Store(tmp_path / "db.sqlite")
    for i in range(5):
        s.create_run("dag", f"2026-11-1{i}")
    page = s.list_runs(limit=2, offset=2)
    assert len(page) == 2


def test_default_stale_constant_is_one_hour():
    assert DEFAULT_STALE_AFTER == 3600


def test_opening_legacy_db_builds_lock_index(tmp_path):
    # 旧版本库没有活动锁索引，重新打开应自动补齐且不丢数据。
    import sqlite3
    db = tmp_path / "legacy.sqlite"
    conn = sqlite3.connect(db)
    conn.executescript(
        "CREATE TABLE runs (run_id TEXT PRIMARY KEY, dag TEXT NOT NULL, "
        "ds TEXT NOT NULL, status TEXT NOT NULL, started_at REAL NOT NULL, "
        "finished_at REAL);"
        "CREATE TABLE task_runs (run_id TEXT, task TEXT, attempt INTEGER, "
        "status TEXT, started_at REAL, finished_at REAL, error TEXT, "
        "PRIMARY KEY (run_id, task, attempt));"
        "INSERT INTO runs VALUES ('r1','dag','2026-11-14','success',1.0,2.0);"
    )
    conn.commit(); conn.close()

    s = Store(db)
    names = {r[0] for r in s.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index'").fetchall()}
    assert "idx_runs_active" in names
    assert s.run_detail("r1")["run"]["status"] == "success"
    # 锁索引可用：同窗口活动运行互斥。
    rid = s.acquire_run("dag", "2026-11-14")
    with pytest.raises(RunAlreadyRunning):
        s.acquire_run("dag", "2026-11-14")
    s.finish_run(rid, "success")
