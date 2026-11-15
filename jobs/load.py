"""示例任务：装载。把转换结果写到最终目录，并打印行数。"""

import json
import os
from pathlib import Path

ds = os.environ.get("WEAVE_DS", "unknown")
base = Path("/tmp/weave/data")
rows = json.loads((base / f"transform_{ds}.json").read_text(encoding="utf-8"))
final = base / "final"
final.mkdir(parents=True, exist_ok=True)
(final / f"{ds}.json").write_text(json.dumps(rows), encoding="utf-8")
print(f"loaded {len(rows)} rows into final/{ds}.json")
