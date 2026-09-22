from __future__ import annotations

import sys
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
CLIENT_ROOT = PACKAGE_ROOT.parent / "treedb_client"
REPO_ROOT = PACKAGE_ROOT.parents[2]
for path in (PACKAGE_ROOT / "src", CLIENT_ROOT / "src"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))
