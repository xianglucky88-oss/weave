"""示例任务：reduce。读取上游实例汇总，按分区合并产物。

完成条件是上游逻辑任务 status=success（即全部实例成功）；若收到
部分完成（理论上不会就绪），直接失败而不是静默产出残缺结果。
"""

import json
import os
from pathlib import Path

ds = os.environ.get("WEAVE_DS", "unknown")
summary = json.loads(os.environ.get("WEAVE_UPSTREAM_SUMMARY", "{}"))
up = summary.get("transform", {})
inst = up.get("instances", {})
print(f"aggregate sees upstream {up.get('status')} {inst}")

if up.get("status") != "success":
    raise SystemExit("reduce requires all upstream instances to succeed")
if inst and inst.get("succeeded") != inst.get("total"):
    raise SystemExit(f"partial completion: {inst}")

base = Path("/tmp/weave/data")
rows = []
for key in sorted(up.get("succeeded_keys", [])):
    part = json.loads((base / "shards" / f"{ds}_{key}.json").read_text(encoding="utf-8"))
    rows.extend(part)
rows.sort(key=lambda r: r["id"])

final = base / "final"
final.mkdir(parents=True, exist_ok=True)
(final / f"{ds}.json").write_text(json.dumps(rows), encoding="utf-8")
print(f"aggregated {len(rows)} rows from {len(up.get('succeeded_keys', []))} shards")
