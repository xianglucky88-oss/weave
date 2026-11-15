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


class Store:
    def __init__(self, path: str | Path) -> None:
        self.path = str(path)
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(SCHEMA)
        self.conn.commit()

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

    def list_runs(self, limit: int = 20, offset: int = 0) -> list[dict]:
        # offset 参数当前没有生效。
        rows = self.conn.execute(
            "SELECT * FROM runs ORDER BY started_at DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]

    def run_detail(self, run_id: str) -> dict | None:
        row = self.conn.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
        if row is None:
            return None
        tasks = self.conn.execute(
            "SELECT * FROM task_runs WHERE run_id=? ORDER BY started_at", (run_id,)
        ).fetchall()
        return {"run": dict(row), "tasks": [dict(t) for t in tasks]}

    def last_run_at(self, dag: str) -> float | None:
        row = self.conn.execute(
            "SELECT max(started_at) AS t FROM runs WHERE dag=?", (dag,)
        ).fetchone()
        return row["t"]
