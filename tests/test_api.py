"""HTTP 接口端到端：过滤参数、分页、SLA 摘要与错误处理。"""

from __future__ import annotations

import json
import threading
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path

import pytest

from weave import api, sla
from weave.dag import DAG, Task
from weave.store import Store


def _seed_run(store, run_id, dag, status, started, finished, tasks):
    store.conn.execute(
        "INSERT INTO runs (run_id, dag, ds, status, started_at, finished_at) "
        "VALUES (?,?,?,?,?,?)",
        (run_id, dag, "2026-11-14", status, started, finished),
    )
    for task, attempt, s, f, st in tasks:
        store.conn.execute(
            "INSERT INTO task_runs (run_id, task, attempt, status, started_at, finished_at) "
            "VALUES (?,?,?,?,?,?)",
            (run_id, task, attempt, st, s, f),
        )
    store.conn.commit()


@pytest.fixture()
def server(tmp_path):
    dags_dir = tmp_path / "dags"
    dags_dir.mkdir()
    jobs_dir = tmp_path / "jobs"
    jobs_dir.mkdir()
    (dags_dir / "d.yaml").write_text(
        "name: demo\ntasks:\n"
        "  - name: fast\n    script: f.py\n    sla_seconds: 10\n"
        "  - name: slow\n    script: s.py\n    sla_seconds: 5\n",
        encoding="utf-8",
    )
    store = Store(tmp_path / "db.sqlite")
    # r1：slow 成功但超时（6s > 5s SLA）；r2 全部达标。
    _seed_run(store, "r1", "demo", "success", 100.0, 120.0, [
        ("fast", 1, 100.0, 102.0, "success"),
        ("slow", 1, 100.0, 106.0, "success"),
    ])
    _seed_run(store, "r2", "demo", "failed", 200.0, 205.0, [
        ("fast", 1, 200.0, 201.0, "failed"),
    ])

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), api.Handler)
    api._STORE, api._DAGS_DIR, api._JOBS_DIR = store, dags_dir, jobs_dir
    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()
    yield f"http://127.0.0.1:{httpd.server_address[1]}"
    httpd.shutdown()


def _get(base, path):
    with urllib.request.urlopen(base + path) as resp:
        return resp.status, json.loads(resp.read().decode())


def _get_err(base, path):
    try:
        urllib.request.urlopen(base + path)
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())
    raise AssertionError("expected HTTP error")


def test_runs_default_response_carries_pagination_and_sla(server):
    code, body = _get(server, "/runs")
    assert code == 200
    assert body["total"] == 2 and body["limit"] == 20 and body["offset"] == 0
    assert len(body["runs"]) == 2

    r1 = next(r for r in body["runs"] if r["run_id"] == "r1")
    assert r1["sla_available"] is True
    assert r1["sla"]["violated"] is True
    assert [v["task"] for v in r1["sla"]["violations"]] == ["slow"]
    # 成功运行也可能违规：违规与运行状态相互独立。
    assert r1["status"] == "success"

    r2 = next(r for r in body["runs"] if r["run_id"] == "r2")
    assert r2["sla"]["violated"] is False
    assert r2["sla"]["failed_tasks"] == 1


def test_filters_by_status_dag_and_time(server):
    _, body = _get(server, "/runs?status=failed")
    assert [r["run_id"] for r in body["runs"]] == ["r2"]

    _, body = _get(server, "/runs?dag=demo&limit=1&offset=1")
    assert body["total"] == 2
    assert [r["run_id"] for r in body["runs"]] == ["r1"]

    _, body = _get(server, "/runs?start_at=150&end_at=250")
    assert [r["run_id"] for r in body["runs"]] == ["r2"]

    # ISO 8601 时间端点（1970-01-01T00:02:20Z = 140 秒）。
    _, body = _get(server, "/runs?start_at=1970-01-01T00:02:20Z")
    assert body["total"] == 1


def test_sla_filter_is_post_filter_with_stable_conclusions(server):
    _, violated = _get(server, "/runs?sla=violated")
    assert [r["run_id"] for r in violated["runs"]] == ["r1"]
    _, ok = _get(server, "/runs?sla=ok")
    assert [r["run_id"] for r in ok["runs"]] == ["r2"]

    # 同一份 SQLite 记录重复查询，SLA 结论逐字节稳定。
    _, again = _get(server, "/runs?sla=violated")
    assert again == violated


def test_unknown_dag_returns_empty_not_all_runs(server):
    _, body = _get(server, "/runs?dag=missing")
    assert body["runs"] == [] and body["total"] == 0


def test_bad_params_return_400(server):
    for path in (
        "/runs?limit=0", "/runs?limit=abc", "/runs?offset=-5",
        "/runs?status=weird", "/runs?start_at=not-a-time",
        "/runs?start_at=10&end_at=5", "/runs?sla=banana",
    ):
        code, body = _get_err(server, path)
        assert code == 400, path
        assert "error" in body


def test_offset_beyond_range(server):
    code, body = _get(server, "/runs?offset=999")
    assert code == 200 and body["runs"] == [] and body["total"] == 2


def test_status_endpoint_includes_sla_detail(server):
    code, body = _get(server, "/status/r1")
    assert code == 200
    assert body["run"]["run_id"] == "r1"
    assert body["sla"]["sla_violated"] is True
    slow = body["sla"]["tasks"]["slow"]
    assert slow["busy_seconds"] == 6.0 and slow["attempts"] == 1

    code, _ = _get_err(server, "/status/nope")
    assert code == 404


def test_sla_summary_matches_direct_module_call(server):
    # 验收：HTTP 摘要与直接对同一份 SQLite 记录调用 sla 模块的结论一致。
    store = api._STORE
    dag = DAG(name="demo", tasks=[
        Task(name="fast", script="f.py", sla_seconds=10),
        Task(name="slow", script="s.py", sla_seconds=5),
    ])
    detail = store.run_detail("r1")
    direct = sla.summarize(dag, detail["tasks"])
    _, body = _get(server, "/status/r1")
    assert body["sla"] == direct
