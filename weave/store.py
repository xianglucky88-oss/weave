"""运行状态持久化（SQLite）。"""

from __future__ import annotations

import sqlite3
import time
import uuid
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id      TEXT PRIMARY KEY,
    dag         TEXT NOT NULL,
    ds          TEXT NOT NULL,
    status      TEXT NOT NULL,
    started_at  REAL NOT NULL,
    finished_at REAL
);
CREATE TABLE IF NOT EXISTS task_runs (
    run_id      TEXT NOT NULL,
    task        TEXT NOT NULL,
    attempt     INTEGER NOT NULL,
    status      TEXT NOT NULL,
    started_at  REAL,
    finished_at REAL,
    error       TEXT,
    PRIMARY KEY (run_id, task, attempt)
);
"""

# 过滤与排序允许的状态白名单，非法值由调用方提前拒绝。
RUN_STATUSES = ("running", "success", "failed")
_MAX_VARS = 500


class Store:
    def __init__(self, path: str | Path) -> None:
        self.path = str(path)
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        # WAL 允许查询与写入并发；busy_timeout 避免短暂锁冲突直接报错。
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA busy_timeout=5000")
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    def _read_conn(self) -> sqlite3.Connection:
        """只读连接：打开的是当前已提交状态的快照。

        每次查询使用独立连接并在事务内完成 count + 取数，查询期间
        其他线程新写入的运行不会出现在本次结果里，分页因此不会
        重复或跳行。
        """
        conn = sqlite3.connect(f"file:{self.path}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    def create_run(self, dag: str, ds: str) -> str:
        run_id = uuid.uuid4().hex[:12]
        self.conn.execute(
            "INSERT INTO runs (run_id, dag, ds, status, started_at) VALUES (?,?,?,?,?)",
            (run_id, dag, ds, "running", time.time()),
        )
        self.conn.commit()
        return run_id

    def set_task(self, run_id: str, task: str, attempt: int, status: str,
                 error: str | None = None, started: float | None = None) -> None:
        now = time.time()
        self.conn.execute(
            """INSERT INTO task_runs (run_id, task, attempt, status, started_at, finished_at, error)
               VALUES (?,?,?,?,?,?,?)
               ON CONFLICT (run_id, task, attempt) DO UPDATE SET
                 status=excluded.status, finished_at=excluded.finished_at, error=excluded.error""",
            (run_id, task, attempt, status, started or now, None if status == "running" else now, error),
        )
        self.conn.commit()

    def statuses(self, run_id: str) -> dict[str, str]:
        rows = self.conn.execute(
            "SELECT task, status FROM task_runs WHERE run_id=? ORDER BY attempt", (run_id,)
        ).fetchall()
        return {r["task"]: r["status"] for r in rows}

    def finish_run(self, run_id: str, status: str) -> None:
        self.conn.execute(
            "UPDATE runs SET status=?, finished_at=? WHERE run_id=?", (status, time.time(), run_id)
        )
        self.conn.commit()

    def list_runs(self, limit: int = 20, offset: int = 0, *,
                  dag: str | None = None, status: str | None = None,
                  start_at: float | None = None,
                  end_at: float | None = None) -> dict:
        """按 DAG / 状态 / 时间范围筛选并分页查询运行。

        返回 {"runs": [...], "total": 匹配总数, "limit": ..., "offset": ...}。
        排序固定为 (started_at DESC, run_id DESC)，配合 run_id 做次序
        兜底，保证 started_at 相同时分页依然确定。
        """
        if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
            raise ValueError("limit must be a positive integer")
        if not isinstance(offset, int) or isinstance(offset, bool) or offset < 0:
            raise ValueError("offset must be a non-negative integer")
        if status is not None and status not in RUN_STATUSES:
            raise ValueError(f"status must be one of {RUN_STATUSES}")

        where: list[str] = []
        params: list = []
        if dag is not None:
            where.append("dag = ?")
            params.append(dag)
        if status is not None:
            where.append("status = ?")
            params.append(status)
        if start_at is not None:
            where.append("started_at >= ?")
            params.append(start_at)
        if end_at is not None:
            where.append("started_at <= ?")
            params.append(end_at)
        clause = (" WHERE " + " AND ".join(where)) if where else ""
        order = " ORDER BY started_at DESC, run_id DESC"

        conn = self._read_conn()
        try:
            # 一个事务内完成 count 与取数：两者看到同一个快照。
            conn.execute("BEGIN")
            total = conn.execute(
                f"SELECT count(*) AS n FROM runs{clause}", params
            ).fetchone()["n"]
            rows = conn.execute(
                f"SELECT * FROM runs{clause}{order} LIMIT ? OFFSET ?",
                [*params, limit, offset],
            ).fetchall()
            conn.commit()
        finally:
            conn.close()
        return {
            "runs": [dict(r) for r in rows],
            "total": total,
            "limit": limit,
            "offset": offset,
        }

    def run_tasks(self, run_ids: list[str]) -> list[dict]:
        """批量读取若干运行的 task_runs 记录，供 SLA 计算使用。"""
        if not run_ids:
            return []
        out: list[dict] = []
        conn = self._read_conn()
        try:
            for i in range(0, len(run_ids), _MAX_VARS):
                chunk = run_ids[i:i + _MAX_VARS]
                placeholders = ",".join("?" for _ in chunk)
                rows = conn.execute(
                    f"SELECT * FROM task_runs WHERE run_id IN ({placeholders}) "
                    "ORDER BY run_id, task, attempt",
                    chunk,
                ).fetchall()
                out.extend(dict(r) for r in rows)
        finally:
            conn.close()
        return out

    def run_detail(self, run_id: str) -> dict | None:
        row = self.conn.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
        if row is None:
            return None
        tasks = self.conn.execute(
            "SELECT * FROM task_runs WHERE run_id=? ORDER BY started_at, task, attempt", (run_id,)
        ).fetchall()
        return {"run": dict(row), "tasks": [dict(t) for t in tasks]}

    def last_run_at(self, dag: str) -> float | None:
        row = self.conn.execute(
            "SELECT max(started_at) AS t FROM runs WHERE dag=?", (dag,)
        ).fetchone()
        return row["t"]
