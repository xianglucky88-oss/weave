"""Store 的过滤、分页与快照一致性测试。"""

from __future__ import annotations

import threading

import pytest

from weave.store import Store


def _seed(store, dag, status, started, finished=None, run_id=None):
    """直接写一条带可控时间戳的运行记录。"""
    rid = run_id or f"r{len(store.conn.execute('SELECT 1 FROM runs').fetchall())}"
    store.conn.execute(
        "INSERT INTO runs (run_id, dag, ds, status, started_at, finished_at) "
        "VALUES (?,?,?,?,?,?)",
        (rid, dag, "2026-11-14", status, started, finished),
    )
    store.conn.commit()
    return rid


def _task(store, run_id, task, attempt, started, finished, status="success"):
    store.conn.execute(
        "INSERT INTO task_runs (run_id, task, attempt, status, started_at, finished_at) "
        "VALUES (?,?,?,?,?,?)",
        (run_id, task, attempt, status, started, finished),
    )
    store.conn.commit()


def test_offset_and_limit_paginate_stably(tmp_path):
    store = Store(tmp_path / "db.sqlite")
    # started_at 全部相同，验证 run_id 兜底排序仍然确定、不重不漏。
    for i in range(5):
        _seed(store, "d", "success", 100.0, run_id=f"r{i}")

    page1 = store.list_runs(limit=2, offset=0)
    page2 = store.list_runs(limit=2, offset=2)
    page3 = store.list_runs(limit=2, offset=4)

    assert page1["total"] == 5
    ids = [r["run_id"] for r in page1["runs"]] + \
          [r["run_id"] for r in page2["runs"]] + \
          [r["run_id"] for r in page3["runs"]]
    assert len(ids) == 5
    assert len(set(ids)) == 5
    # 相同 started_at 下按 run_id 倒序。
    assert ids == sorted(ids, reverse=True)
    assert page1["limit"] == 2 and page1["offset"] == 0


def test_offset_beyond_range_returns_empty_but_keeps_total(tmp_path):
    store = Store(tmp_path / "db.sqlite")
    _seed(store, "d", "success", 1.0)
    page = store.list_runs(limit=20, offset=999)
    assert page["runs"] == []
    assert page["total"] == 1


def test_filters_by_dag_status_and_time_range(tmp_path):
    store = Store(tmp_path / "db.sqlite")
    _seed(store, "dag_a", "success", 10.0)
    _seed(store, "dag_a", "failed", 20.0)
    _seed(store, "dag_b", "success", 30.0)

    assert store.list_runs(dag="dag_a")["total"] == 2
    assert store.list_runs(status="failed")["total"] == 1
    assert store.list_runs(dag="dag_b", status="success")["total"] == 1
    assert store.list_runs(dag="dag_b", status="failed")["total"] == 0
    # 时间范围按 started_at，闭区间语义。
    assert store.list_runs(start_at=15.0)["total"] == 2
    assert store.list_runs(end_at=20.0)["total"] == 2
    assert store.list_runs(start_at=11.0, end_at=25.0)["total"] == 1
    assert store.list_runs(start_at=10.0, end_at=10.0)["total"] == 1


def test_invalid_pagination_arguments_rejected(tmp_path):
    store = Store(tmp_path / "db.sqlite")
    with pytest.raises(ValueError):
        store.list_runs(limit=0)
    with pytest.raises(ValueError):
        store.list_runs(offset=-1)
    with pytest.raises(ValueError):
        store.list_runs(limit=True)
    with pytest.raises(ValueError):
        store.list_runs(status="bogus")


def test_concurrent_writes_do_not_change_in_flight_query(tmp_path):
    store = Store(tmp_path / "db.sqlite")
    for i in range(10):
        _seed(store, "d", "success", float(i))

    seen_during_query = []
    release = threading.Event()

    def writer():
        # 等查询拿到快照后再写入 20 条新运行。
        release.wait()
        for j in range(20):
            _seed(store, "d", "success", 1000.0 + j, run_id=f"w{j}")

    t = threading.Thread(target=writer)
    t.start()

    orig = store._read_conn

    def instrumented():
        release.set()
        return orig()

    store._read_conn = instrumented
    page = store.list_runs(limit=50, offset=0)
    t.join()

    # 查询快照在写入前已固定：total 与行集都不包含新写入。
    assert page["total"] == 10
    assert len(page["runs"]) == 10
    # 写入确实落库了，后续查询能看到。
    assert store.list_runs()["total"] == 30


def test_run_tasks_batches_and_orders(tmp_path):
    store = Store(tmp_path / "db.sqlite")
    rid1 = _seed(store, "d", "failed", 1.0, run_id="r1")
    rid2 = _seed(store, "d", "success", 2.0, run_id="r2")
    _task(store, rid1, "a", 1, 1.0, 2.0, "failed")
    _task(store, rid1, "a", 2, 3.0, 4.0)
    _task(store, rid2, "b", 1, 2.0, 3.0)

    rows = store.run_tasks([rid1, rid2])
    assert len(rows) == 3
    assert [(r["run_id"], r["task"], r["attempt"]) for r in rows] == [
        ("r1", "a", 1), ("r1", "a", 2), ("r2", "b", 1),
    ]
    assert store.run_tasks([]) == []
