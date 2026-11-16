"""HTTP 查询接口（标准库实现，无第三方依赖）。"""

from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from .check import check_file, find_dag_file
from .runner import run_dag
from .store import Store
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

    def do_GET(self):  # noqa: N802
        u = urlparse(self.path)
        q = parse_qs(u.query)

        if u.path == "/health":
            return self._json(200, {"status": "ok"})

        if u.path == "/dags":
            out = []
            for p in sorted(_DAGS_DIR.glob("*.yaml")):
                r = check_file(p, jobs_dir=_JOBS_DIR)
                out.append({
                    "name": r.dag_name,
                    "file": p.name,
                    "tasks": [t.name for t in r.dag.tasks] if r.dag else [],
                    "schedule": r.dag.schedule if r.dag else "",
                    "ok": r.ok,
                    "problems": [f"{d.code}: {d.message}" for d in r.errors],
                })
            return self._json(200, {"dags": out})

        if u.path == "/runs":
            limit = int(q.get("limit", ["20"])[0])
            offset = int(q.get("offset", ["0"])[0])
            # offset 目前被忽略。
            return self._json(200, {"runs": _STORE.list_runs(limit=limit, offset=offset)})

        if u.path.startswith("/status/"):
            run_id = u.path.split("/status/", 1)[1]
            detail = _STORE.run_detail(run_id)
            if detail is None:
                return self._json(404, {"error": "run not found"})
            return self._json(200, detail)

        return self._json(404, {"error": "not found"})

    def do_POST(self):  # noqa: N802
        u = urlparse(self.path)
        if u.path.startswith("/run/"):
            name = u.path.split("/run/", 1)[1]
            try:
                path = find_dag_file(name, _DAGS_DIR)
            except FileNotFoundError:
                return self._json(404, {"error": "dag not found"})
            report = check_file(path, jobs_dir=_JOBS_DIR)
            if not report.ok:
                return self._json(422, {
                    "error": "invalid dag",
                    "problems": [f"{d.code}: {d.message}" for d in report.errors],
                })
            ds = schedule.ds_of(__import__("time").time())
            run_id, statuses = run_dag(report.dag, _STORE, ds, _JOBS_DIR)
            return self._json(200, {"run_id": run_id, "statuses": statuses})
        return self._json(404, {"error": "not found"})


def serve(store: Store, dags_dir: Path, jobs_dir: Path, port: int = 9700) -> None:
    global _STORE, _DAGS_DIR, _JOBS_DIR
    _STORE, _DAGS_DIR, _JOBS_DIR = store, dags_dir, jobs_dir
    httpd = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print(f"weave api listening on http://127.0.0.1:{port}")
    httpd.serve_forever()
