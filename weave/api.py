"""HTTP 查询接口（标准库实现，无第三方依赖）。"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from .dag import load, validate
from .runner import run_dag
from .store import RUN_STATUSES, Store
from . import schedule, sla

_STORE: Store | None = None
_DAGS_DIR: Path | None = None
_JOBS_DIR: Path | None = None

# 单页硬上限，防止调用方一次拖走整张表。
MAX_LIMIT = 200
DEFAULT_LIMIT = 20


def _first(q: dict, key: str) -> str | None:
    values = q.get(key)
    if not values:
        return None
    return values[0]


def parse_int(q: dict, key: str, default: int, *, minimum: int,
              maximum: int | None = None) -> int:
    """解析整数查询参数，非法值抛 ValueError 并由路由转 400。"""
    raw = _first(q, key)
    if raw is None:
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{key} must be an integer") from exc
    if value < minimum or (maximum is not None and value > maximum):
        rng = f"{minimum}..{maximum}" if maximum is not None else f">= {minimum}"
        raise ValueError(f"{key} out of range ({rng})")
    return value


def parse_time(q: dict, key: str) -> float | None:
    """解析时间范围端点：支持 epoch 秒或 ISO 8601（Z 结尾按 UTC）。"""
    raw = _first(q, key)
    if raw is None or raw == "":
        return None
    try:
        return float(raw)
    except ValueError:
        pass
    text = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
    try:
        dt = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(
            f"{key} must be epoch seconds or ISO 8601 timestamp"
        ) from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def load_dags() -> dict:
    """按 DAG 名加载目录里的全部定义。"""
    out = {}
    for p in sorted(_DAGS_DIR.glob("*.yaml")):
        d = load(p)
        out[d.name] = d
    return out


def enrich_run(run: dict, dags: dict, tasks_by_run: dict[str, list[dict]]) -> dict:
    """给一条运行记录附加 SLA 摘要。

    找不到 DAG 定义（已删除/重命名）时无法计算 SLA，显式标注
    sla_available=false，而不是猜测为无违规。
    """
    dag = dags.get(run["dag"])
    if dag is None:
        run["sla_available"] = False
        return run
    summary = sla.summarize(dag, tasks_by_run.get(run["run_id"], []))
    run["sla_available"] = True
    run["sla"] = {
        "violated": summary["sla_violated"],
        "violation_count": summary["violation_count"],
        "violations": summary["violations"],
        "wall_seconds": summary["wall_seconds"],
        "total_task_seconds": summary["total_task_seconds"],
        "failed_tasks": summary["failed_tasks"],
        "task_count": summary["task_count"],
    }
    return run


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
            return self._handle_runs(q)

        if u.path.startswith("/status/"):
            run_id = u.path.split("/status/", 1)[1]
            detail = _STORE.run_detail(run_id)
            if detail is None:
                return self._json(404, {"error": "run not found"})
            dags = load_dags()
            dag = dags.get(detail["run"]["dag"])
            if dag is not None:
                detail["sla"] = sla.summarize(dag, detail["tasks"])
                detail["sla_available"] = True
            else:
                detail["sla_available"] = False
            return self._json(200, detail)

        return self._json(404, {"error": "not found"})

    def _handle_runs(self, q: dict) -> None:
        # 参数校验：任何非法输入返回 400 而不是静默吞掉。
        try:
            limit = parse_int(q, "limit", DEFAULT_LIMIT, minimum=1, maximum=MAX_LIMIT)
            offset = parse_int(q, "offset", 0, minimum=0)
            start_at = parse_time(q, "start_at")
            end_at = parse_time(q, "end_at")
        except ValueError as exc:
            return self._json(400, {"error": str(exc)})

        if start_at is not None and end_at is not None and start_at > end_at:
            return self._json(400, {"error": "start_at must be <= end_at"})

        dag_name = _first(q, "dag")
        status = _first(q, "status")
        if status is not None and status not in RUN_STATUSES:
            return self._json(400, {"error": f"status must be one of {list(RUN_STATUSES)}"})

        sla_filter = _first(q, "sla")
        if sla_filter is not None and sla_filter not in ("violated", "ok"):
            return self._json(400, {"error": "sla must be 'violated' or 'ok'"})

        dags = load_dags()
        if dag_name is not None and dag_name not in dags:
            # 未知 DAG：结果确定为空，而不是忽略过滤条件。
            return self._json(200, {
                "runs": [], "total": 0, "limit": limit, "offset": offset,
                "filters": {"dag": dag_name},
            })

        page = _STORE.list_runs(
            limit=limit, offset=offset, dag=dag_name, status=status,
            start_at=start_at, end_at=end_at,
        )

        tasks_by_run: dict[str, list[dict]] = {}
        for rec in _STORE.run_tasks([r["run_id"] for r in page["runs"]]):
            tasks_by_run.setdefault(rec["run_id"], []).append(rec)
        runs = [enrich_run(run, dags, tasks_by_run) for run in page["runs"]]

        if sla_filter is not None:
            wanted = sla_filter == "violated"
            runs = [r for r in runs
                    if r.get("sla_available") and r["sla"]["violated"] == wanted]

        return self._json(200, {
            "runs": runs,
            "total": page["total"],
            "limit": limit,
            "offset": offset,
            "filters": {
                "dag": dag_name,
                "status": status,
                "start_at": start_at,
                "end_at": end_at,
                "sla": sla_filter,
            },
        })

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
            ds = schedule.ds_of(time.time())
            run_id, statuses = run_dag(dag, _STORE, ds, _JOBS_DIR)
            return self._json(200, {"run_id": run_id, "statuses": statuses})
        return self._json(404, {"error": "not found"})


def serve(store: Store, dags_dir: Path, jobs_dir: Path, port: int = 9700) -> None:
    global _STORE, _DAGS_DIR, _JOBS_DIR
    _STORE, _DAGS_DIR, _JOBS_DIR = store, dags_dir, jobs_dir
    httpd = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print(f"weave api listening on http://127.0.0.1:{port}")
    httpd.serve_forever()
