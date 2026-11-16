"""动态任务展开（map/reduce 扇出）。

一个声明了 ``expand`` 的任务会在运行时被展开成多个 :class:`TaskInstance`。
实例的身份由 **key** 决定，而不是由它在列表中的位置决定：这样在同一个 run
被重启、展开结果发生增删或重排时，已经成功的实例仍然可以被识别并跳过。

实例 key 的推导顺序（见 :func:`_key_for`）::

    1. expand.key 指定的字段；
    2. partition 里的 "key" 字段；
    3. partition 里的 "id" 字段；
    4. 前三种都不存在时，对 partition 的规范化 JSON 取 sha256 前缀。

实例展示名默认为 ``<task>[<key>]``，可通过 ``expand.name`` 模板覆盖
（支持 ``{task}`` / ``{key}``）。非展开任务只有一个实例，key 为空字符串，
展示名就是任务名。
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


class ExpansionError(Exception):
    """展开失败（源缺失、格式错误、key 重复等）。"""


@dataclass(frozen=True)
class ExpandSpec:
    """DAG 中的 ``expand`` 声明。

    两个来源二选一：

    * ``items``：DAG 里内联的分区列表；
    * ``path``：运行时读取的 JSON 文件，路径支持 ``{ds}`` / ``{task}``。
    """

    items: tuple[Any, ...] | None = None
    path: str | None = None
    key: str | None = None
    name: str | None = None


@dataclass
class TaskInstance:
    """一次具体的任务执行。

    ``key`` 是稳定身份（落库、增量重跑去重的依据），``name`` 是展示名，
    ``params`` 是运行时参数快照（基础参数 + 分区字段 + 上游汇总）。
    """

    name: str
    params: dict[str, Any] = field(default_factory=dict)
    key: str = ""


def parse_spec(raw: Any) -> ExpandSpec:
    """把 YAML 里的 ``expand`` 解析成 :class:`ExpandSpec`。

    支持三种写法::

        expand: [ ... ]              # 等价于 items
        expand: partitions.json      # 等价于 path
        expand:                      # 完整写法
          items: [...]
          # 或
          path: /data/partitions_{ds}.json
          key: id
          name: "transform[{key}]"
    """
    if isinstance(raw, list):
        return ExpandSpec(items=tuple(raw))
    if isinstance(raw, str):
        return ExpandSpec(path=raw)
    if not isinstance(raw, dict):
        raise ValueError(f"expand must be a mapping, list or string, got {type(raw).__name__}")

    unknown = set(raw) - {"items", "path", "key", "name"}
    if unknown:
        raise ValueError(f"unknown expand keys: {', '.join(sorted(unknown))}")

    has_items = "items" in raw
    has_path = "path" in raw
    if has_items == has_path:
        raise ValueError("expand must declare exactly one of 'items' or 'path'")
    if has_items:
        if not isinstance(raw["items"], list):
            raise ValueError("expand.items must be a list")
        items = tuple(raw["items"])
        path = None
    else:
        if not isinstance(raw["path"], str) or not raw["path"]:
            raise ValueError("expand.path must be a non-empty string")
        items = None
        path = raw["path"]

    key = raw.get("key")
    if key is not None and not isinstance(key, str):
        raise ValueError("expand.key must be a string")
    name = raw.get("name")
    if name is not None and not isinstance(name, str):
        raise ValueError("expand.name must be a string")
    return ExpandSpec(items=items, path=path, key=key, name=name)


def validate_spec(spec: ExpandSpec) -> list[str]:
    """校验展开声明中可以静态检查的部分。"""
    problems: list[str] = []
    if spec.items is None and spec.path is None:
        problems.append("expand must declare 'items' or 'path'")
    if spec.items is not None and spec.path is not None:
        problems.append("expand cannot declare both 'items' and 'path'")
    if spec.path is not None and not spec.path:
        problems.append("expand.path must not be empty")

    if spec.items is not None:
        seen: set[str] = set()
        for idx, part in enumerate(spec.items):
            try:
                _, key = _partition(part, spec, idx)
            except ExpansionError as exc:
                problems.append(str(exc))
                continue
            if key in seen:
                problems.append(f"duplicate expand key {key!r}")
            seen.add(key)

    if spec.name:
        try:
            _render_name(spec.name, "task", "key")
        except (KeyError, IndexError, ValueError) as exc:
            problems.append(f"invalid expand.name template: {exc}")
    return problems


def params_digest(params: dict[str, Any]) -> str:
    """参数快照的稳定摘要，用于检测实例定义是否变化。"""
    raw = json.dumps(params, ensure_ascii=False, sort_keys=True,
                     separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def expansion_digest(instances: list[TaskInstance]) -> str:
    """整个展开结果的摘要（按 key 排序），用于审计与变化检测。"""
    payload = [
        [inst.key, params_digest(inst.params), inst.name]
        for inst in sorted(instances, key=lambda i: i.key)
    ]
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def expand(task, ds: str) -> list[TaskInstance]:
    """把任务展开成实例列表。

    未声明 ``expand`` 的任务返回单个实例，key 为空字符串，保持向后兼容。
    声明了 ``expand`` 的任务在每次调用时重新读取来源；空列表是合法的，
    表示“零个分区”，由调用方按空展开语义（成功）处理。
    """
    base = dict(getattr(task, "params", {}) or {})
    spec = getattr(task, "expand", None)
    if spec is None:
        return [TaskInstance(name=task.name, params=base, key="")]

    partitions = _load_partitions(spec, ds, task.name)
    out: list[TaskInstance] = []
    seen: set[str] = set()
    for idx, part in enumerate(partitions):
        extra, key = _partition(part, spec, idx)
        if key in seen:
            raise ExpansionError(
                f"task {task.name!r}: duplicate expand key {key!r} at index {idx}"
            )
        seen.add(key)
        params = {**base, **extra}
        out.append(TaskInstance(
            name=_render_name(spec.name or f"{task.name}[{{key}}]", task.name, key),
            params=params,
            key=key,
        ))
    return out


def _load_partitions(spec: ExpandSpec, ds: str, task_name: str) -> list[Any]:
    if spec.items is not None:
        return list(spec.items)
    assert spec.path is not None
    path = spec.path.replace("{ds}", ds).replace("{task}", task_name)
    p = Path(path)
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ExpansionError(f"expand source not found for task {task_name!r}: {path}") from exc
    except OSError as exc:
        raise ExpansionError(f"cannot read expand source {path}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise ExpansionError(f"invalid JSON in expand source {path}: {exc}") from exc
    if isinstance(raw, dict):
        # 允许上游产物使用 {"partitions": [...]} 或 {"items": [...]} 包装。
        for key in ("partitions", "items"):
            if isinstance(raw.get(key), list):
                return list(raw[key])
        raise ExpansionError(f"expand source {path} must be a JSON list")
    if not isinstance(raw, list):
        raise ExpansionError(f"expand source {path} must be a JSON list")
    return raw


def _partition(part: Any, spec: ExpandSpec, idx: int) -> tuple[dict[str, Any], str]:
    if isinstance(part, dict):
        params = dict(part)
    else:
        params = {"value": part}

    key: Any
    if spec.key:
        if not isinstance(part, dict) or spec.key not in part:
            raise ExpansionError(
                f"expand item at index {idx} is missing key field {spec.key!r}"
            )
        key = part[spec.key]
    elif isinstance(part, dict) and "key" in part:
        key = part["key"]
    elif isinstance(part, dict) and "id" in part:
        key = part["id"]
    else:
        key = params_digest(params)[:12]

    key_str = str(key)
    if not key_str:
        raise ExpansionError(f"expand item at index {idx} has an empty key")
    return params, key_str


def _render_name(template: str, task_name: str, key: str) -> str:
    return template.replace("{task}", task_name).replace("{key}", key)
