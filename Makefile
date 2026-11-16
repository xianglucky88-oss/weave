SHELL := /bin/sh
PY ?= python3

.PHONY: help test demo verify verify-resume list clean

help:
	@echo "make test           运行测试"
	@echo "make demo           跑一次 demo（含展开与续跑）"
	@echo "make verify         断言正常运行与展开结果"
	@echo "make verify-resume  断言部分完成/续跑的一致性"
	@echo "make list           列出 DAG"
	@echo "make clean          清理数据"

test:
	$(PY) -m pytest -q

demo:
	sh scripts/demo.sh

verify:
	sh scripts/verify.sh

verify-resume:
	sh scripts/verify_resume.sh

list:
	$(PY) -m weave list

clean:
	rm -rf /tmp/weave .pytest_cache
