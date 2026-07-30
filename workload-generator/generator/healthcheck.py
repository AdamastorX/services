"""Exec liveness probe (backlog #45's own resource-governance AC, applied
to this component too -- see platform/kubernetes/workload-generator/
deployment.yaml). No HTTP server in this process (deliberately the
simplest thing that works -- a script, not a service), so the probe can't
be `httpGet` the way api/workers' Spring Actuator ones are; `exec`-ing this
script and checking the heartbeat file's mtime is the equivalent check.

generator.run's main loop touches HEARTBEAT_PATH at least once a second
*regardless* of the configured rate (a near-zero rate still ticks every
second, it just skips the actual HTTP request) -- so a stale heartbeat
means the process is genuinely stuck or dead, never confusable with "rate
turned down".
"""

from __future__ import annotations

import os
import sys
import time

HEARTBEAT_PATH = os.environ.get("HEARTBEAT_PATH", "/tmp/workload-generator-heartbeat")
MAX_AGE_SECONDS = float(os.environ.get("HEARTBEAT_MAX_AGE_SECONDS", "30"))


def main() -> int:
    try:
        mtime = os.path.getmtime(HEARTBEAT_PATH)
    except OSError:
        return 1
    age = time.time() - mtime
    return 0 if age < MAX_AGE_SECONDS else 1


if __name__ == "__main__":
    sys.exit(main())
