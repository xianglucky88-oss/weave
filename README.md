# weave

轻量 DAG 调度器：解析依赖、判定就绪、执行任务、记录状态。

## 快速开始

```bash
pip install -r requirements.txt
make demo     # 跑一个正常 DAG 和一个失败 DAG
make verify   # 断言结果
make test     # 单元测试
python3 -m weave serve --port 9700   # HTTP 查询接口
```

## 结构

```
weave/dag.py        DAG 解析、校验、拓扑序
weave/ready.py      就绪判定、失败传播
weave/schedule.py   定时触发与 catchup
weave/expand.py     动态任务展开
weave/retry.py      重试与退避
weave/rerun.py      历史重跑
weave/sla.py        SLA 检查
weave/store.py      SQLite 状态存储
weave/executor.py   子进程执行任务
weave/api.py        HTTP 接口
dags/               DAG 定义
jobs/               任务脚本
```

## 写一个任务

任务就是一个 Python 脚本，通过环境变量拿到上下文：

- `WEAVE_RUN_ID`：本次运行 ID
- `WEAVE_DS`：数据日期
- `WEAVE_PARAM_<NAME>`：DAG 里声明的参数

## 扩展方式

- 新的调度语义：改 `schedule.py` 的 `is_due` / `catchup_windows`
- 新的重试策略：改 `retry.py` 的 `delay_for`
- 任务扇出：改 `expand.py`，`runner` 已经按实例列表执行
- 新的存储后端：`store.py` 的接口保持不变即可替换
