"""示例任务：转换。读取抽取结果，补一个字段。"""

import json
import os
from pathlib import Path

ds = os.environ.get("WEAVE_DS", "unknown")
base = Path("/tmp/weave/data")
rows = json.loads((base / f"extract_{ds}.json").read_text(encoding="utf-8"))
for r in rows:
    r["amount_with_tax"] = round(r["amount"] * 1.06, 2)
(base / f"transform_{ds}.json").write_text(json.dumps(rows), encoding="utf-8")
print(f"transformed {len(rows)} rows")
