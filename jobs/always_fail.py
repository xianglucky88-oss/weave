"""示例任务：总是失败。用于演示失败传播。"""

import sys

print("simulated failure", file=sys.stderr)
sys.exit(1)
