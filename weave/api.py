"""HTTP 查询接口（标准库实现，无第三方依赖）。"""

from __future__ import annotations

import json
import signal
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from .dag import load, validate
from .runner import run_dag
from .store import Store
from . import schedule

_STORE: Store | None = None
_DAGS_DIR: Path | None = None
_JOBS_DIR: Path | None = None
# 服务级停止事件：信号置位后，进行中的运行会中断退避并清理子进程。
_SHUTDOWN: threading.Event | None = None


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
                d = load(p)
                out.append({"name": d.name, "tasks": [t.name for t in d.tasks],
                            "schedule": d.schedule, "problems": validate(d)})
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
            dag = None
            for p in sorted(_DAGS_DIR.glob("*.yaml")):
                d = load(p)
                if d.name == name:
                    dag = d
                    break
            if dag is None:
                return self._json(404, {"error": "dag not found"})
            problems = validate(dag)
            if problems:
                return self._json(422, {"error": "invalid dag", "problems": problems})
            ds = schedule.ds_of(__import__("time").time())
            run_id, statuses = run_dag(dag, _STORE, ds, _JOBS_DIR,
                                       shutdown=_SHUTDOWN)
            return self._json(200, {"run_id": run_id, "statuses": statuses})
        return self._json(404, {"error": "not found"})


def serve(store: Store, dags_dir: Path, jobs_dir: Path, port: int = 9700) -> None:
    global _STORE, _DAGS_DIR, _JOBS_DIR, _SHUTDOWN
    _STORE, _DAGS_DIR, _JOBS_DIR = store, dags_dir, jobs_dir
    _SHUTDOWN = threading.Event()

    httpd = ThreadingHTTPServer(("127.0.0.1", port), Handler)

    def _ask_stop(signum, _frame):
        # 信号在主线程送达，而 serve_forever 也跑在主线程；
        # shutdown() 必须由另一个线程调用，否则与 serve_forever 互锁。
        _SHUTDOWN.set()
        threading.Thread(target=httpd.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, _ask_stop)
    signal.signal(signal.SIGINT, _ask_stop)

    print(f"weave api listening on http://127.0.0.1:{port}")
    try:
        httpd.serve_forever()
    finally:
        # 等待在途请求（可能正在跑 DAG）结束，再关闭监听套接字。
        httpd.server_close()
        print("weave api stopped")
