"""展开声明解析与实例身份推导的测试。"""

from __future__ import annotations

import json

import pytest

from weave.dag import DAG, Task, validate
from weave.expand import (
    ExpansionError,
    TaskInstance,
    expand,
    expansion_digest,
    params_digest,
    parse_spec,
)


def test_non_expand_task_has_single_legacy_instance():
    task = Task(name="a", script="a.py", params={"x": "1"})
    out = expand(task, "2026-11-14")
    assert out == [TaskInstance(name="a", params={"x": "1"}, key="")]


def test_inline_items_naming_key_and_params():
    task = Task(
        name="transform", script="t.py", params={"stage": "map"},
        expand=parse_spec({
            "items": [{"shard": "b", "n": 2}, {"shard": "a", "n": 1}],
            "key": "shard",
            "name": "transform[{key}]",
        }),
    )
    out = expand(task, "2026-11-14")
    assert [i.name for i in out] == ["transform[b]", "transform[a]"]
    assert [i.key for i in out] == ["b", "a"]
    # 参数快照 = 任务基础参数 + 分区字段
    assert out[0].params == {"stage": "map", "shard": "b", "n": 2}


def test_path_source_and_ds_substitution(tmp_path):
    src = tmp_path / "partitions_2026-11-14.json"
    src.write_text(json.dumps([{"id": 7}, {"id": 9}]), encoding="utf-8")
    task = Task(
        name="m", script="m.py",
        expand=parse_spec({"path": str(tmp_path / "partitions_{ds}.json")}),
    )
    out = expand(task, "2026-11-14")
    assert [i.key for i in out] == ["7", "9"]
    # 未指定 name 模板时使用 task[key]
    assert [i.name for i in out] == ["m[7]", "m[9]"]


def test_path_source_accepts_wrapped_list(tmp_path):
    src = tmp_path / "p.json"
    src.write_text(json.dumps({"partitions": [{"key": "x"}]}), encoding="utf-8")
    task = Task(name="m", script="m.py",
                expand=parse_spec({"path": str(src)}))
    assert [i.key for i in expand(task, "d")] == ["x"]


def test_missing_source_is_expansion_error(tmp_path):
    task = Task(name="m", script="m.py",
                expand=parse_spec({"path": str(tmp_path / "missing.json")}))
    with pytest.raises(ExpansionError):
        expand(task, "d")


def test_empty_expansion_is_empty_list():
    task = Task(name="m", script="m.py", expand=parse_spec({"items": []}))
    assert expand(task, "d") == []


def test_duplicate_keys_rejected_at_validate_time():
    dag = DAG(name="d", tasks=[Task(
        name="m", script="m.py",
        expand=parse_spec({"items": [{"id": 1}, {"id": 1}]}),
    )])
    assert any("duplicate expand key" in p for p in validate(dag))


def test_duplicate_keys_rejected_at_runtime_for_path(tmp_path):
    src = tmp_path / "p.json"
    src.write_text(json.dumps([{"id": 1}, {"id": 1}]), encoding="utf-8")
    task = Task(name="m", script="m.py",
                expand=parse_spec({"path": str(src), "key": "id"}))
    with pytest.raises(ExpansionError, match="duplicate expand key"):
        expand(task, "d")


def test_missing_key_field_is_error(tmp_path):
    task = Task(name="m", script="m.py",
                expand=parse_spec({"items": [{"shard": "a"}], "key": "id"}))
    with pytest.raises(ExpansionError, match="missing key field"):
        expand(task, "d")


def test_scalar_items_get_stable_hashed_key():
    task = Task(name="m", script="m.py",
                expand=parse_spec({"items": ["a", "b"]}))
    out = expand(task, "d")
    assert len({i.key for i in out}) == 2
    assert [i.key for i in out] == [params_digest({"value": "a"})[:12],
                                    params_digest({"value": "b"})[:12]]
    # 顺序变化不会改变每个分区的身份
    shuffled = expand(Task(name="m", script="m.py",
                          expand=parse_spec({"items": ["b", "a"]})), "d")
    assert {i.key: i.name for i in out} == {i.key: i.name for i in shuffled}


def test_digest_is_order_independent_but_param_sensitive():
    a = [TaskInstance("m[1]", {"x": 1}, "1"), TaskInstance("m[2]", {"x": 2}, "2")]
    b = list(reversed(a))
    assert expansion_digest(a) == expansion_digest(b)
    c = [TaskInstance("m[1]", {"x": 1}, "1"), TaskInstance("m[2]", {"x": 3}, "2")]
    assert expansion_digest(a) != expansion_digest(c)
    assert params_digest({"x": 1}) == params_digest({"x": 1})


def test_parse_spec_rejects_conflicting_sources():
    with pytest.raises(ValueError):
        parse_spec({"items": [], "path": "/x"})
    with pytest.raises(ValueError):
        parse_spec({"items": [], "bogus": 1})


def test_name_template_requires_key():
    dag = DAG(name="d", tasks=[Task(
        name="m", script="m.py",
        expand=parse_spec({"items": [{"id": 1}], "name": "fixed"}),
    )])
    assert any("must contain" in p for p in validate(dag))


def test_validate_accepts_valid_expand():
    dag = DAG(name="d", tasks=[Task(
        name="m", script="m.py",
        expand=parse_spec({"items": [{"id": 1}, {"id": 2}], "key": "id"}),
    )])
    assert validate(dag) == []
