"""HTTP 查询接口（标准库实现，无第三方依赖）。"""

from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from .dag import load, validate
from .rerun import RerunConfigError, execute_plan, plan_reruns
from .runner import run_dag
from .schedule import ScheduleError
from .store import RunAlreadyRunning, Store
from . import schedule

_STORE: Store | None = None
_DAGS_DIR: Path | None = None
_JOBS_DIR: Path | None = None


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):  # 静默，避免污染 demo 输出
        pass

    def _json(self, code: int, body: dict) -> None:
        raw = json.dumps(body, ensure_ascii=False, default=str).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _body(self) -> dict:
        """解析 JSON 或表单请求体；空体返回 {}。"""
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        ctype = self.headers.get("Content-Type", "")
        if "application/json" in ctype:
            try:
                data = json.loads(raw.decode("utf-8"))
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid json body: {exc}") from exc
            if not isinstance(data, dict):
                raise ValueError("json body must be an object")
            return data
        return {k: v[0] for k, v in parse_qs(raw.decode("utf-8")).items()}

    def do_GET(self):  # noqa: N802
        u = urlparse(self.path)
        q = parse_qs(u.query)

        if u.path == "/health":
            return self._json(200, {"status": "ok"})

        if u.path == "/dags":
            out = []
            for p in sorted(_DAGS_DIR.glob("*.yaml")):
                d = load(p)
                out.append({"name": d.name, "tasks": [t.name for t in d.tasks],
                            "schedule": d.schedule, "problems": validate(d)})
            return self._json(200, {"dags": out})

        if u.path == "/runs":
            limit = int(q.get("limit", ["20"])[0])
            offset = int(q.get("offset", ["0"])[0])
            return self._json(200, {"runs": _STORE.list_runs(limit=limit, offset=offset)})

        if u.path.startswith("/status/"):
            run_id = u.path.split("/status/", 1)[1]
            detail = _STORE.run_detail(run_id)
            if detail is None:
                return self._json(404, {"error": "run not found"})
            return self._json(200, detail)

        return self._json(404, {"error": "not found"})

    def _find_dag(self, name: str):
        for p in sorted(_DAGS_DIR.glob("*.yaml")):
            d = load(p)
            if d.name == name:
                return d
        return None

    def _handle_window_request(self, name: str, body: dict, *, mode: str):
        """catchup / rerun 共用的规划+执行逻辑，输出逐窗口解释。"""
        dag = self._find_dag(name)
        if dag is None:
            return self._json(404, {"error": "dag not found"})
        problems = validate(dag)
        if problems:
            return self._json(422, {"error": "invalid dag", "problems": problems})

        try:
            kwargs = {"on_exists": body.get("on_exists", "retry")}
            if "stale_after" in body:
                kwargs["stale_after"] = float(body["stale_after"])
            if "max_windows" in body:
                kwargs["max_windows"] = int(body["max_windows"])
            plan = plan_reruns(dag, _STORE, start=body.get("start"),
                               end=body.get("end"), **kwargs)
        except (RerunConfigError, ScheduleError) as exc:
            return self._json(400, {"error": str(exc)})

        dry_run = bool(body.get("dry_run", False))
        if dry_run:
            return self._json(200, {"plan": plan.to_dict()})

        report = execute_plan(
            plan, dag, _STORE, _JOBS_DIR,
            timeout=int(body.get("timeout", 60)),
            continue_on_fail=bool(body.get("continue_on_fail", False)),
        )
        code = 200
        if any(e.status in ("failed", "interrupted") for e in report.entries) or report.stopped_early:
            code = 409
        return self._json(code, {"plan": plan.to_dict(), "execution": report.to_dict()})

    def do_POST(self):  # noqa: N802
        u = urlparse(self.path)
        try:
            body = self._body()
        except ValueError as exc:
            return self._json(400, {"error": str(exc)})

        if u.path.startswith("/catchup/"):
            name = u.path.split("/catchup/", 1)[1]
            return self._handle_window_request(name, body, mode="catchup")

        if u.path.startswith("/rerun/"):
            name = u.path.split("/rerun/", 1)[1]
            return self._handle_window_request(name, body, mode="rerun")

        if u.path.startswith("/run/"):
            name = u.path.split("/run/", 1)[1]
            dag = self._find_dag(name)
            if dag is None:
                return self._json(404, {"error": "dag not found"})
            problems = validate(dag)
            if problems:
                return self._json(422, {"error": "invalid dag", "problems": problems})
            ds_raw = body.get("ds")
            sched = dag.schedule or "day"
            try:
                if ds_raw:
                    lo, _ = schedule.parse_bound(sched, ds_raw)
                    ds = schedule.window_ds(sched, lo)
                else:
                    ds = schedule.window_ds(sched, schedule.time.time())
            except ScheduleError as exc:
                return self._json(400, {"error": str(exc)})
            try:
                run_id, statuses = run_dag(
                    dag, _STORE, ds, _JOBS_DIR,
                    timeout=int(body.get("timeout", 60)),
                    guard=not bool(body.get("no_guard", False)),
                )
            except RunAlreadyRunning as exc:
                return self._json(409, {
                    "error": "window already running",
                    "ds": ds, "run_id": exc.run_id, "reason": str(exc),
                })
            return self._json(200, {"run_id": run_id, "ds": ds, "statuses": statuses})
        return self._json(404, {"error": "not found"})


def serve(store: Store, dags_dir: Path, jobs_dir: Path, port: int = 9700) -> None:
    global _STORE, _DAGS_DIR, _JOBS_DIR
    _STORE, _DAGS_DIR, _JOBS_DIR = store, dags_dir, jobs_dir
    httpd = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print(f"weave api listening on http://127.0.0.1:{port}")
    httpd.serve_forever()
