"""DAG 解析与校验的测试。"""

from __future__ import annotations

import pytest

from weave import dag as dagmod
from weave.dag import load, validate


def write(tmp_path, text):
    p = tmp_path / "d.yaml"
    p.write_text(text, encoding="utf-8")
    return p


def test_load_and_order(tmp_path):
    p = write(tmp_path, """
name: demo
tasks:
  - name: a
    script: a.py
  - name: b
    script: b.py
    depends_on: [a]
""")
    d = load(p)
    assert d.name == "demo"
    assert validate(d) == []
    assert d.order() == ["a", "b"]


def test_cycle_detected(tmp_path):
    p = write(tmp_path, """
name: cyc
tasks:
  - name: a
    script: a.py
    depends_on: [b]
  - name: b
    script: b.py
    depends_on: [a]
""")
    d = load(p)
    problems = validate(d)
    assert any("cycle" in x for x in problems)


def test_unknown_dependency(tmp_path):
    p = write(tmp_path, """
name: bad
tasks:
  - name: a
    script: a.py
    depends_on: [missing]
""")
    d = load(p)
    assert any("unknown" in x for x in validate(d))


def test_duplicate_names(tmp_path):
    p = write(tmp_path, """
name: dup
tasks:
  - name: a
    script: a.py
  - name: a
    script: b.py
""")
    d = load(p)
    assert any("duplicate" in x for x in validate(d))
