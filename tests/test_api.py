"""HTTP 接口的端到端测试：run 防重、catchup、rerun、dry-run。"""

from __future__ import annotations

import json
import socket
import threading
import urllib.request
from http.server import ThreadingHTTPServer

import pytest

from weave.api import Handler
from weave.store import Store


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class ApiHarness:
    def __init__(self, tmp_path):
        dags = tmp_path / "dags"
        jobs = tmp_path / "jobs"
        dags.mkdir()
        jobs.mkdir()
        (jobs / "ok.py").write_text("print('ok')\n", encoding="utf-8")
        (jobs / "bad.py").write_text("import sys; sys.exit(2)\n", encoding="utf-8")
        (dags / "orders.yaml").write_text(
            "name: orders\nschedule: day\n"
            "tasks:\n  - name: a\n    script: ok.py\n", encoding="utf-8")
        (dags / "broken.yaml").write_text(
            "name: broken\nschedule: day\n"
            "tasks:\n  - name: a\n    script: bad.py\n", encoding="utf-8")

        self.store = Store(tmp_path / "db.sqlite")
        port = free_port()
        self.base = f"http://127.0.0.1:{port}"
        self.httpd = ThreadingHTTPServer(("127.0.0.1", port), Handler)
        # 复用 api 模块的全局装配。
        import weave.api as api_mod
        api_mod._STORE = self.store
        api_mod._DAGS_DIR = dags
        api_mod._JOBS_DIR = jobs
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    def stop(self):
        self.httpd.shutdown()

    def post(self, path, body=None):
        data = json.dumps(body or {}).encode()
        req = urllib.request.Request(
            self.base + path, data=data,
            headers={"Content-Type": "application/json"}, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return resp.status, json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read())

    def get(self, path):
        with urllib.request.urlopen(self.base + path, timeout=10) as resp:
            return resp.status, json.loads(resp.read())


@pytest.fixture
def api(tmp_path):
    h = ApiHarness(tmp_path)
    yield h
    h.stop()


def test_run_then_duplicate_run_is_rejected(api):
    # 先占住一个仍在执行的窗口（模拟长任务进行中的重复提交）。
    holder = api.store.acquire_run("orders", "2026-11-10")
    code, body = api.post("/run/orders", {"ds": "2026-11-10"})
    assert code == 409
    assert body["error"] == "window already running"
    assert body["run_id"] == holder

    # 运行终结后，同 ds 允许重新触发（retry/force 语义）。
    api.store.finish_run(holder, "success")
    code, body = api.post("/run/orders", {"ds": "2026-11-10"})
    assert code == 200 and body["statuses"] == {"a": "success"}


def test_rerun_dry_run_explains_each_window(api):
    code, body = api.post("/rerun/orders", {
        "start": "2026-11-10", "end": "2026-11-12", "dry_run": True})
    assert code == 200
    plan = body["plan"]
    assert plan["mode"] == "rerun"
    assert plan["summary"]["to_run"] == 3
    assert all(w["reason"] for w in plan["windows"])


def test_catchup_executes_and_second_call_is_noop(api):
    code, body = api.post("/rerun/orders", {
        "start": "2026-11-10", "end": "2026-11-11"})
    assert code == 200
    assert body["execution"]["summary"]["succeeded"] == 2

    # 用已成功的日期做锚点 catchup：只有开放/未来窗口，全部跳过。
    code, body = api.post("/catchup/orders", {"on_exists": "retry"})
    assert code == 200
    assert body["execution"]["summary"]["executed"] == 0
    assert all(w["action"] == "skip" for w in body["plan"]["windows"])


def test_rerun_failure_returns_conflict_and_force_regenerates(api):
    code, body = api.post("/rerun/broken", {
        "start": "2026-11-10", "end": "2026-11-10"})
    assert code == 409
    assert body["execution"]["results"][0]["status"] == "failed"

    # retry 再次规划：失败窗口仍然要跑。
    code, body = api.post("/rerun/broken", {
        "start": "2026-11-10", "end": "2026-11-10", "dry_run": True})
    assert body["plan"]["windows"][0]["state"] == "failed"
    assert body["plan"]["windows"][0]["action"] == "run"

    # skip 策略则排除该窗口。
    code, body = api.post("/rerun/broken", {
        "start": "2026-11-10", "end": "2026-11-10",
        "on_exists": "skip", "dry_run": True})
    assert body["plan"]["windows"][0]["action"] == "skip"


def test_bad_requests(api):
    code, body = api.post("/rerun/orders", {"start": "nope", "end": "2026-11-10"})
    assert code == 400 and "error" in body
    code, body = api.post("/catchup/does_not_exist")
    assert code == 404
    code, body = api.post("/run/orders", {"ds": "2026-11-10", "no_guard": True})
    assert code == 200  # 显式关闭锁时允许再建运行
