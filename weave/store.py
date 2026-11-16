"""运行状态持久化（SQLite）。"""

from __future__ import annotations

import sqlite3
import threading
import time
import uuid
from pathlib import Path

# 运行超过该秒数仍停留在 running，视为进程崩溃留下的陈旧锁。
DEFAULT_STALE_AFTER = 3600

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id      TEXT PRIMARY KEY,
    dag         TEXT NOT NULL,
    ds          TEXT NOT NULL,
    status      TEXT NOT NULL,
    started_at  REAL NOT NULL,
    finished_at REAL
);
CREATE INDEX IF NOT EXISTS idx_runs_dag_ds ON runs (dag, ds);
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

# 一个 (dag, ds) 同时只允许存在一个活动（未终结）的运行——这是重复提交的
# 数据库级防线：第二个 acquire_run 会直接命中唯一约束。
ACTIVE_LOCK_INDEX = (
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_runs_active "
    "ON runs (dag, ds) WHERE status = 'running'"
)


class RunAlreadyRunning(Exception):
    """同一 (dag, ds) 已有活动运行，拒绝重复创建。"""

    def __init__(self, run_id: str, started_at: float) -> None:
        super().__init__(f"active run {run_id} already holds this window")
        self.run_id = run_id
        self.started_at = started_at


class Store:
    def __init__(
        self, path: str | Path, stale_after: float = DEFAULT_STALE_AFTER
    ) -> None:
        self.path = str(path)
        self.stale_after = stale_after
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        # check_same_thread=False 供 HTTP 线程池使用；进程内用一把可重入锁
        # 串行化写事务，SQLite 文件锁再兜底跨进程并发。
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(self.path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        with self._lock:
            self.conn.executescript(SCHEMA)
            self._enable_active_lock()
            self.conn.commit()

    def _enable_active_lock(self) -> None:
        """建活动锁唯一索引；先把历史遗留的陈旧 running 行标记为 interrupted。"""
        stale_before = time.time() - self.stale_after
        self.conn.execute(
            "UPDATE runs SET status='interrupted', finished_at=? "
            "WHERE status='running' AND started_at < ?",
            (time.time(), stale_before),
        )
        try:
            self.conn.execute(ACTIVE_LOCK_INDEX)
        except sqlite3.IntegrityError:
            # 仍有未过期的活动行（例如另一个进程正在跑）：清理本轮能清的，
            # 锁索引留到下次初始化再建。
            pass

    def create_run(self, dag: str, ds: str) -> str:
        """无条件创建运行（历史调用保持原语义）。新代码优先用 acquire_run。"""
        run_id = uuid.uuid4().hex[:12]
        self.conn.execute(
            "INSERT INTO runs (run_id, dag, ds, status, started_at) VALUES (?,?,?,?,?)",
            (run_id, dag, ds, "running", time.time()),
        )
        self.conn.commit()
        return run_id

    def acquire_run(self, dag: str, ds: str) -> str:
        """幂等地为 (dag, ds) 获取一个活动运行。

        - 存在未过期的 running 行：抛 RunAlreadyRunning，调用方据此跳过；
        - 存在已过期的 running 行（上次执行进程被杀）：先改判 interrupted
          再创建新运行，实现“计划中途失败”的接管；
        - 没有活动行：插入新运行。活动锁唯一索引兜底并发重复提交。
        """
        stale_before = time.time() - self.stale_after
        with self._lock:
            try:
                with self.conn:  # 单事务，保证检查-清理-插入的原子性
                    row = self.conn.execute(
                        "SELECT run_id, started_at FROM runs "
                        "WHERE dag=? AND ds=? AND status='running' "
                        "ORDER BY started_at DESC LIMIT 1",
                        (dag, ds),
                    ).fetchone()
                    if row is not None:
                        if row["started_at"] >= stale_before:
                            raise RunAlreadyRunning(row["run_id"], row["started_at"])
                        self.conn.execute(
                            "UPDATE runs SET status='interrupted', finished_at=? "
                            "WHERE run_id=?",
                            (time.time(), row["run_id"]),
                        )
                    run_id = uuid.uuid4().hex[:12]
                    self.conn.execute(
                        "INSERT INTO runs (run_id, dag, ds, status, started_at) "
                        "VALUES (?,?,?,?,?)",
                        (run_id, dag, ds, "running", time.time()),
                    )
                    return run_id
            except sqlite3.IntegrityError as exc:
                # 并发情况下另一个进程抢先持有了活动锁（进程内已被 RLock 挡住）。
                row = self.conn.execute(
                    "SELECT run_id, started_at FROM runs "
                    "WHERE dag=? AND ds=? AND status='running' "
                    "ORDER BY started_at DESC LIMIT 1",
                    (dag, ds),
                ).fetchone()
                if row is not None:
                    raise RunAlreadyRunning(row["run_id"], row["started_at"]) from exc
                raise

    def mark_run_interrupted(self, run_id: str) -> None:
        """把陈旧活动运行改判为 interrupted。"""
        self.conn.execute(
            "UPDATE runs SET status='interrupted', finished_at=? "
            "WHERE run_id=? AND status='running'",
            (time.time(), run_id),
        )
        self.conn.commit()

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
        rows = self.conn.execute(
            "SELECT * FROM runs ORDER BY started_at DESC LIMIT ? OFFSET ?",
            (limit, offset),
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

    def latest_run_per_ds(self, dag: str, ds_values: list[str]) -> dict[str, dict]:
        """每个 ds 取该 DAG 最新一条运行记录（用于补跑规划的状态判定）。

        返回 {ds: {run_id, ds, status, started_at, finished_at, age}}；
        没有任何运行的 ds 不出现在结果里。
        """
        if not ds_values:
            return {}
        now = time.time()
        placeholders = ",".join("?" for _ in ds_values)
        rows = self.conn.execute(
            f"""SELECT r.* FROM runs r
                JOIN (
                    SELECT ds, max(started_at) AS latest
                    FROM runs WHERE dag=? AND ds IN ({placeholders})
                    GROUP BY ds
                ) m ON r.ds = m.ds AND r.started_at = m.latest
                WHERE r.dag=?""",
            [dag, *ds_values, dag],
        ).fetchall()
        out: dict[str, dict] = {}
        for row in rows:
            rec = dict(row)
            rec["age"] = now - rec["started_at"]
            out[rec["ds"]] = rec
        return out

    def active_run(self, dag: str, ds: str) -> dict | None:
        """返回该窗口当前未终结的运行；没有则 None。"""
        row = self.conn.execute(
            "SELECT * FROM runs WHERE dag=? AND ds=? AND status='running' "
            "ORDER BY started_at DESC LIMIT 1",
            (dag, ds),
        ).fetchone()
        return dict(row) if row is not None else None
