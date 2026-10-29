# #4519 supervisor custody repair

Successor to a6f9fceea; synthetic supervisor evidence only. Hosted semantic
qualification remains separate from controlled performance qualification. This
change provides no controlled-host admission and runs no product benchmarks.
All earlier failures and qualification thresholds remain unchanged.

The prior supervisor reaped the capture leader before its final killpg, allowing
PID/group reuse and allowing successful completion before descendants stopped.
The repaired Linux supervisor observes exit with waitid(WNOWAIT), retaining the
leader PID through every group signal. It checks child custody before each
signal, drains live group members through /proc, and reaps only after its final
signal. Zombies count as terminated, not running work. A lost/reaped child ID
is never signalled. Failed observation or termination is incomplete even when
best-effort cleanup subsequently succeeds. Receipt publication uses a pending
file and atomic rename; write failure returns 125 and emits an incomplete error
instead of returning successful qualification.

Validation on 2026-10-28:

- `python -W error::ResourceWarning scripts/treedb_vacuum_m0_ci_test.py`: 9 tests pass.
- `git diff --check`: passes.

Tests use real processes for ordinary success/failure, timeout, public-shell
interruption, leader success with a TERM-resistant descendant, externally reaped
custody, failed drain, failed termination, and failed receipt write. Faults are
injected only at the specific observation/signal/write boundary. The custody
assertion checks WNOWAIT immediately before every real group signal. The reaped
child test exercises the precondition for recycled IDs without trying to force
host-wide PID reuse. No unrelated process group is signalled.
