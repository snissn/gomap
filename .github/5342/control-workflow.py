#!/usr/bin/env python3
# Source-only control; execution requires admitted, retained task directory.
from pathlib import Path
import subprocess
base = Path(__file__).resolve().parent
workflow = (base / 'candidate/.github/workflows/read-snapshot-guardrails.yml').read_text()
original = subprocess.check_output(['git', 'show', '0538bf2dc25519bbeabe968d35b025c240f4d4f7:.github/workflows/read-snapshot-guardrails.yml'], text=True)
expected = original.replace('            go.sum\n', '            go.sum\n            .github/workflows/unified-bench-tests.yml\n').replace('      - name: Setup Go\n        uses:', '      - name: Setup Go\n        id: setup_go\n        uses:').replace('      - name: Run read-snapshot append-only guardrail\n', '''      - name: Observe dependency cache metadata
        env:
          SETUP_GO_CACHE_HIT: ${{ steps.setup_go.outputs.cache-hit }}
        run: |
          if timeout --kill-after=1s 5s bash ./scripts/observe_read_snapshot_cache.sh; then
            echo "[cache-observation] status=complete"
          else
            echo "[cache-observation] status=incomplete"
          fi

      - name: Run read-snapshot append-only guardrail
''')
assert workflow == expected, 'unexpected workflow change or order'
assert original.count('run: ./scripts/check_read_snapshot_guardrail.sh') == 1
print('PASS: exact workflow additions; original guardrail and timeout retained')
