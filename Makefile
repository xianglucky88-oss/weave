SHELL := /bin/sh
PY ?= python3

.PHONY: help test demo verify list clean

help:
	@echo "make test     运行测试"
	@echo "make demo     跑一次 orders_daily"
	@echo "make verify   断言运行结果"
	@echo "make list     列出 DAG"
	@echo "make clean    清理数据"

test:
	$(PY) -m pytest -q

demo:
	sh scripts/demo.sh

verify:
	sh scripts/verify.sh

list:
	$(PY) -m weave list

clean:
	rm -rf /tmp/weave .pytest_cache
