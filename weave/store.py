"""运行状态持久化（SQLite）。

存储分三层，避免把"逻辑任务"和"实例"混为一谈：

``runs``
    一次 DAG 运行。
``logical_tasks``
    每个逻辑任务在一 run 内的聚合状态、展开摘要、实例数。
``task_instances``
    每个实例的每次尝试（attempt）。一个实例由 ``(task, instance_key)``
    标识，重复执行（重试 / 增量重跑）追加 attempt，不覆盖历史。

``task_runs`` 是 ``task_instances`` 上的兼容视图，保留旧的查询写法。
"""

from __future__ import annotations

import json
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
CREATE TABLE IF NOT EXISTS logical_tasks (
    run_id          TEXT NOT NULL,
    task            TEXT NOT NULL,
    status          TEXT NOT NULL,
    expand_digest   TEXT,
    instance_count  INTEGER,
    error           TEXT,
    started_at      REAL,
    finished_at     REAL,
    PRIMARY KEY (run_id, task)
);
CREATE TABLE IF NOT EXISTS task_instances (
    run_id          TEXT NOT NULL,
    task            TEXT NOT NULL,
    instance_key    TEXT NOT NULL,
    instance_name   TEXT NOT NULL,
    attempt         INTEGER NOT NULL,
    status          TEXT NOT NULL,
    params_json     TEXT NOT NULL,
    params_digest   TEXT NOT NULL,
    active          INTEGER NOT NULL DEFAULT 1,
    started_at      REAL,
    finished_at     REAL,
    error           TEXT,
    PRIMARY KEY (run_id, task, instance_key, attempt)
);
CREATE INDEX IF NOT EXISTS idx_task_instances_run
    ON task_instances (run_id, task, instance_key, attempt);
"""

LEGACY_VIEW = """
CREATE VIEW IF NOT EXISTS task_runs AS
SELECT run_id, task, attempt, status, started_at, finished_at, error,
       instance_key, instance_name, params_json, params_digest, active
FROM task_instances;
"""


class Store:
    def __init__(self, path: str | Path) -> None:
        self.path = str(path)
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(SCHEMA)
        self._migrate_legacy()
        self.conn.execute(LEGACY_VIEW)
        self.conn.commit()

    # ---- 迁移 ---------------------------------------------------------

    def _migrate_legacy(self) -> None:
        """把旧版 ``task_runs`` 表迁移成 ``task_instances`` + 视图。

        旧记录没有实例概念，统一迁移为 ``instance_key=''`` 的实例。
        """
        row = self.conn.execute(
            "SELECT type FROM sqlite_master WHERE name='task_runs'"
        ).fetchone()
        if row is None or row["type"] != "table":
            return

        self.conn.execute("ALTER TABLE task_runs RENAME TO task_runs_legacy")
        self.conn.execute(
            """INSERT OR IGNORE INTO task_instances
               (run_id, task, instance_key, instance_name, attempt, status,
                params_json, params_digest, active, started_at, finished_at, error)
               SELECT run_id, task, '', task, attempt, status,
                      '{}', '', 1, started_at, finished_at, error
               FROM task_runs_legacy"""
        )
        self.conn.execute("DROP TABLE task_runs_legacy")
        self.conn.commit()

    # ---- run ----------------------------------------------------------

    def create_run(self, dag: str, ds: str) -> str:
        run_id = uuid.uuid4().hex[:12]
        self.conn.execute(
            "INSERT INTO runs (run_id, dag, ds, status, started_at) VALUES (?,?,?,?,?)",
            (run_id, dag, ds, "running", time.time()),
        )
        self.conn.commit()
        return run_id

    def get_run(self, run_id: str) -> dict | None:
        row = self.conn.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
        return dict(row) if row else None

    def find_run(self, dag: str, ds: str) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM runs WHERE dag=? AND ds=? ORDER BY started_at DESC LIMIT 1",
            (dag, ds),
        ).fetchone()
        return dict(row) if row else None

    def reopen_run(self, run_id: str) -> None:
        """重开一个已结束（或中断）的 run，用于增量重跑。"""
        self.conn.execute(
            "UPDATE runs SET status='running', finished_at=NULL WHERE run_id=?",
            (run_id,),
        )
        self.conn.commit()

    def finish_run(self, run_id: str, status: str) -> None:
        self.conn.execute(
            "UPDATE runs SET status=?, finished_at=? WHERE run_id=?",
            (status, time.time(), run_id),
        )
        self.conn.commit()

    # ---- 逻辑任务 ------------------------------------------------------

    def init_logical(self, run_id: str, tasks: list[str]) -> None:
        now = time.time()
        for task in tasks:
            self.conn.execute(
                """INSERT OR IGNORE INTO logical_tasks
                   (run_id, task, status, started_at) VALUES (?,?,?,?)""",
                (run_id, task, "pending", now),
            )
        self.conn.commit()

    def prepare_resume(self, run_id: str, tasks: list[str]) -> None:
        """把逻辑任务重置为 pending；实例级历史保持不变。"""
        self.init_logical(run_id, tasks)
        for task in tasks:
            self.conn.execute(
                """UPDATE logical_tasks
                   SET status='pending', finished_at=NULL, error=NULL
                   WHERE run_id=? AND task=?""",
                (run_id, task),
            )
        self.conn.commit()

    def set_logical(self, run_id: str, task: str, status: str, *,
                    expand_digest: str | None = None,
                    instance_count: int | None = None,
                    error: str | None = None) -> None:
        now = time.time()
        terminal = status in ("success", "failed", "skipped")
        row = self.conn.execute(
            "SELECT started_at FROM logical_tasks WHERE run_id=? AND task=?",
            (run_id, task),
        ).fetchone()
        started = row["started_at"] if row else now
        self.conn.execute(
            """INSERT INTO logical_tasks
               (run_id, task, status, expand_digest, instance_count, error,
                started_at, finished_at)
               VALUES (?,?,?,?,?,?,?,?)
               ON CONFLICT (run_id, task) DO UPDATE SET
                 status=excluded.status,
                 expand_digest=COALESCE(excluded.expand_digest, logical_tasks.expand_digest),
                 instance_count=COALESCE(excluded.instance_count, logical_tasks.instance_count),
                 error=excluded.error,
                 finished_at=excluded.finished_at""",
            (run_id, task, status, expand_digest, instance_count, error,
             started or now, now if terminal else None),
        )
        self.conn.commit()

    def logical(self, run_id: str) -> dict[str, dict]:
        rows = self.conn.execute(
            "SELECT * FROM logical_tasks WHERE run_id=?", (run_id,)
        ).fetchall()
        return {r["task"]: dict(r) for r in rows}

    # ---- 实例 ----------------------------------------------------------

    def next_attempt(self, run_id: str, task: str, instance_key: str) -> int:
        row = self.conn.execute(
            """SELECT COALESCE(MAX(attempt), 0) + 1 AS n FROM task_instances
               WHERE run_id=? AND task=? AND instance_key=?""",
            (run_id, task, instance_key),
        ).fetchone()
        return int(row["n"])

    def begin_instance(self, run_id: str, task: str, instance_key: str,
                       instance_name: str, attempt: int, params: dict,
                       params_digest: str,
                       status: str = "running") -> None:
        now = time.time()
        self.conn.execute(
            """INSERT INTO task_instances
               (run_id, task, instance_key, instance_name, attempt, status,
                params_json, params_digest, active, started_at, finished_at)
               VALUES (?,?,?,?,?,?,?,?,1,?,?)""",
            (run_id, task, instance_key, instance_name, attempt, status,
             json.dumps(params, ensure_ascii=False, sort_keys=True, default=str),
             params_digest, now, None if status == "running" else now),
        )
        self.conn.commit()

    def finish_instance(self, run_id: str, task: str, instance_key: str,
                        attempt: int, status: str, error: str | None = None) -> None:
        self.conn.execute(
            """UPDATE task_instances
               SET status=?, finished_at=?, error=?
               WHERE run_id=? AND task=? AND instance_key=? AND attempt=?""",
            (status, time.time(), error, run_id, task, instance_key, attempt),
        )
        self.conn.commit()

    def deactivate_instance(self, run_id: str, task: str, instance_key: str) -> None:
        """实例定义发生变化时，把旧尝试保留为历史但排除出当前展开。"""
        self.conn.execute(
            """UPDATE task_instances SET active=0
               WHERE run_id=? AND task=? AND instance_key=?""",
            (run_id, task, instance_key),
        )
        self.conn.commit()

    def deactivate_missing(self, run_id: str, task: str, keys: set[str]) -> None:
        """把本次展开中已不存在的实例标记为不活跃（保留历史记录）。"""
        if not keys:
            self.conn.execute(
                "UPDATE task_instances SET active=0 WHERE run_id=? AND task=?",
                (run_id, task),
            )
        else:
            placeholders = ",".join("?" for _ in keys)
            self.conn.execute(
                f"""UPDATE task_instances SET active=0
                    WHERE run_id=? AND task=? AND instance_key NOT IN ({placeholders})""",
                (run_id, task, *sorted(keys)),
            )
        self.conn.commit()

    def interrupt_running(self, run_id: str) -> None:
        """上次进程中断时留下的 running 尝试标记为失败，等待重跑。"""
        self.conn.execute(
            """UPDATE task_instances
               SET status='failed', finished_at=?,
                   error=COALESCE(error, 'interrupted: run resumed before completion')
               WHERE run_id=? AND status='running'""",
            (time.time(), run_id),
        )
        self.conn.commit()

    def latest_instances(self, run_id: str, task: str | None = None,
                         active_only: bool = True) -> list[dict]:
        """每个 ``(task, instance_key)`` 的最新一次尝试。"""
        sql = """
            SELECT ti.* FROM task_instances ti
            JOIN (
                SELECT task, instance_key, MAX(attempt) AS m
                FROM task_instances WHERE run_id=?
                GROUP BY task, instance_key
            ) latest
              ON latest.task = ti.task
             AND latest.instance_key = ti.instance_key
             AND latest.m = ti.attempt
            WHERE ti.run_id=?
        """
        args: list = [run_id, run_id]
        if task is not None:
            sql += " AND ti.task=?"
            args.append(task)
        if active_only:
            sql += " AND ti.active=1"
        sql += " ORDER BY ti.task, ti.instance_key"
        return [dict(r) for r in self.conn.execute(sql, args).fetchall()]

    def latest_instance(self, run_id: str, task: str, instance_key: str) -> dict | None:
        for rec in self.latest_instances(run_id, task):
            if rec["instance_key"] == instance_key:
                return rec
        return None

    def instance_attempts(self, run_id: str, task: str | None = None) -> list[dict]:
        sql = "SELECT * FROM task_instances WHERE run_id=?"
        args: list = [run_id]
        if task is not None:
            sql += " AND task=?"
            args.append(task)
        sql += " ORDER BY task, instance_key, attempt"
        return [dict(r) for r in self.conn.execute(sql, args).fetchall()]

    # ---- 兼容查询 ------------------------------------------------------

    def statuses(self, run_id: str) -> dict[str, str]:
        """逻辑任务状态表（旧接口）。旧库没有 logical_tasks 时按实例聚合。"""
        logical = self.logical(run_id)
        if logical:
            return {t: r["status"] for t, r in logical.items()}
        out: dict[str, str] = {}
        for rec in self.latest_instances(run_id):
            cur = out.get(rec["task"])
            if rec["status"] == "failed":
                out[rec["task"]] = "failed"
            elif cur is None:
                out[rec["task"]] = rec["status"]
        return out

    def run_detail(self, run_id: str) -> dict | None:
        row = self.conn.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
        if row is None:
            return None
        attempts = self.instance_attempts(run_id)
        logical = list(self.logical(run_id).values())
        return {
            "run": dict(row),
            # 兼容旧调用方：逐次尝试列表
            "tasks": attempts,
            # 当前展开下的实例（每个实例的最新尝试，仅活跃）
            "instances": self.latest_instances(run_id),
            # 逻辑任务聚合
            "logical_tasks": sorted(logical, key=lambda r: r["task"]),
        }

    def list_runs(self, limit: int = 20, offset: int = 0) -> list[dict]:
        rows = self.conn.execute(
            "SELECT * FROM runs ORDER BY started_at DESC LIMIT ? OFFSET ?",
            (limit, offset),
        ).fetchall()
        return [dict(r) for r in rows]

    def last_run_at(self, dag: str) -> float | None:
        row = self.conn.execute(
            "SELECT max(started_at) AS t FROM runs WHERE dag=?", (dag,)
        ).fetchone()
        return row["t"]
