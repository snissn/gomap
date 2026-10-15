#!/usr/bin/env python3
"""Go -exec seam: retain the actual fresh-process ELF, then replace this process."""
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys
import time


def main():
    destination = Path(os.environ["MVCC_M0_EXEC_DIR"])
    destination.mkdir(parents=True, exist_ok=False)
    source = Path(sys.argv[1])
    started = time.time_ns()
    binary = destination / "actual.test"
    shutil.copy2(source, binary)
    (destination / "exec.json").write_text(json.dumps({
        "argv": sys.argv[1:], "pid": os.getpid(), "pgid": os.getpgrp(),
        "copy_start_ns": started, "exec_wall_ns": time.time_ns(),
        "sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "wrapper_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "overhead": "ELF copy/hash before benchmark setup; diagnostic I/O"
    }, indent=2) + "\n")
    # exec preserves the driver's process group, signals and actual exit status.
    os.execv(str(source), sys.argv[1:])


if __name__ == "__main__":
    main()
