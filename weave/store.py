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
    kind        TEXT NOT NULL DEFAULT '',
    exit_code   INTEGER,
    signal      INTEGER,
    timed_out   INTEGER NOT NULL DEFAULT 0,
    killed      INTEGER NOT NULL DEFAULT 0,
    output_truncated INTEGER NOT NULL DEFAULT 0,
    seconds     REAL,
    PRIMARY KEY (run_id, task, attempt)
);
"""

# 老版本数据库升级：逐列补齐，已存在的列跳过。
_MIGRATIONS = {
    "task_runs": [
        ("kind", "TEXT NOT NULL DEFAULT ''"),
        ("exit_code", "INTEGER"),
        ("signal", "INTEGER"),
        ("timed_out", "INTEGER NOT NULL DEFAULT 0"),
        ("killed", "INTEGER NOT NULL DEFAULT 0"),
        ("output_truncated", "INTEGER NOT NULL DEFAULT 0"),
        ("seconds", "REAL"),
    ],
}


class Store:
    def __init__(self, path: str | Path) -> None:
        self.path = str(path)
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(SCHEMA)
        self._migrate()
        self.conn.commit()

    def _migrate(self) -> None:
        for table, columns in _MIGRATIONS.items():
            existing = {r["name"] for r in self.conn.execute(f"PRAGMA table_info({table})")}
            for name, decl in columns:
                if name not in existing:
                    self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {decl}")

    def _new_run_id(self) -> str:
        """生成 run_id 并消除极小概率的碰撞。

        run_id 是 runs 表主键，插入冲突时换新 id 重试，而不是把上一次
        运行的记录覆盖掉。
        """
        for _ in range(5):
            run_id = uuid.uuid4().hex[:12]
            try:
                self.conn.execute(
                    "INSERT INTO runs (run_id, dag, ds, status, started_at) VALUES (?,?,?,?,?)",
                    (run_id, "", "", "", 0),
                )
                self.conn.rollback()
                return run_id
            except sqlite3.IntegrityError:
                continue
        # 12 位 hex 连续 5 次碰撞在统计上不可能；发生说明环境异常，直接报错
        # 比静默复用别人的 run_id 安全。
        raise RuntimeError("could not allocate unique run_id")

    def create_run(self, dag: str, ds: str) -> str:
        run_id = self._new_run_id()
        self.conn.execute(
            "INSERT INTO runs (run_id, dag, ds, status, started_at) VALUES (?,?,?,?,?)",
            (run_id, dag, ds, "running", time.time()),
        )
        self.conn.commit()
        return run_id

    def set_task(self, run_id: str, task: str, attempt: int, status: str,
                 error: str | None = None, started: float | None = None,
                 kind: str = "", exit_code: int | None = None,
                 signal: int | None = None, timed_out: bool = False,
                 killed: bool = False, output_truncated: bool = False,
                 seconds: float | None = None) -> None:
        now = time.time()
        self.conn.execute(
            """INSERT INTO task_runs (run_id, task, attempt, status, started_at,
                   finished_at, error, kind, exit_code, signal, timed_out, killed,
                   output_truncated, seconds)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT (run_id, task, attempt) DO UPDATE SET
                 status=excluded.status, finished_at=excluded.finished_at,
                 error=excluded.error, kind=excluded.kind,
                 exit_code=excluded.exit_code, signal=excluded.signal,
                 timed_out=excluded.timed_out, killed=excluded.killed,
                 output_truncated=excluded.output_truncated,
                 seconds=excluded.seconds""",
            (run_id, task, attempt, status, started or now,
             None if status == "running" else now, error, kind, exit_code, signal,
             int(timed_out), int(killed), int(output_truncated), seconds),
        )
        self.conn.commit()

    def skip_task(self, run_id: str, task: str, reason: str) -> None:
        """把未执行的下游任务记为 skipped（attempt=0 表示从未尝试）。"""
        now = time.time()
        self.conn.execute(
            """INSERT INTO task_runs (run_id, task, attempt, status, started_at,
                   finished_at, error, kind, seconds)
               VALUES (?,?,?,?,?,?,?,?,?)
               ON CONFLICT (run_id, task, attempt) DO UPDATE SET
                 status=excluded.status, error=excluded.error,
                 finished_at=excluded.finished_at""",
            (run_id, task, 0, "skipped", now, now, reason, "skipped", 0.0),
        )
        self.conn.commit()

    def statuses(self, run_id: str) -> dict[str, str]:
        # 同一任务可能有多条 attempt 记录，取最大 attempt（0 是跳过占位），
        # 这样状态反映最后一次尝试的结局。
        rows = self.conn.execute(
            """SELECT task, status FROM task_runs
               WHERE run_id=? AND attempt=(
                   SELECT max(attempt) FROM task_runs tr2
                   WHERE tr2.run_id=task_runs.run_id AND tr2.task=task_runs.task
               )""",
            (run_id,),
        ).fetchall()
        return {r["task"]: r["status"] for r in rows}

    def finish_run(self, run_id: str, status: str) -> None:
        self.conn.execute(
            "UPDATE runs SET status=?, finished_at=? WHERE run_id=?", (status, time.time(), run_id)
        )
        self.conn.commit()

    def list_runs(self, limit: int = 20, offset: int = 0) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM runs ORDER BY started_at DESC LIMIT ? OFFSET ?",
            (limit, max(0, offset)),
        ).fetchall()
        return [dict(r) for r in rows]

    def run_detail(self, run_id: str) -> dict | None:
        row = self.conn.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
        if row is None:
            return None
        tasks = self.conn.execute(
            "SELECT * FROM task_runs WHERE run_id=? ORDER BY task, attempt", (run_id,)
        ).fetchall()
        return {"run": dict(row), "tasks": [dict(t) for t in tasks]}

    def last_run_at(self, dag: str) -> float | None:
        row = self.conn.execute(
            "SELECT max(started_at) AS t FROM runs WHERE dag=?", (dag,)
        ).fetchone()
        return row["t"]
