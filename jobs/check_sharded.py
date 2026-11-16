"""示例任务：校验 reduce 产物与源数据行数一致。"""

import json
import os
import sys
from pathlib import Path

ds = os.environ.get("WEAVE_DS", "unknown")
base = Path("/tmp/weave/data")
rows = json.loads((base / "final" / f"{ds}.json").read_text(encoding="utf-8"))
src = json.loads((base / f"extract_{ds}.json").read_text(encoding="utf-8"))
if len(rows) != len(src):
    print(f"row count mismatch: {len(src)} vs {len(rows)}", file=sys.stderr)
    sys.exit(1)
print(f"check passed: {len(rows)} rows")
