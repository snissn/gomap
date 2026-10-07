Staged uncommitted fixture-only repair at `/tmp/gomap-r1-5067` / HEAD `0245d8e0f5835dcba2929e4f1ba8b5d7ea909133`.

Adds existing bounded `fixedPeerWaitV1` after replay prefix/durable completion checks, waiting for all nodes prepared/vector/ACTIVE because CATALOG tail replay is independent. Existing assertions and timeout remain. Only other delta is canonical test-discovery SHA. Refresh/check and `git diff --cached --check` exit0. No Go, commit, remote action or GitHub activity.

Patch SHA256 `1cb8a8eb9f50d13627bb26ea328a4f6429b70873824632fd76d54f1bfe054181`. Exact staged blobs/tree and commands in handoff.json. Writer released for root independent review/validation.
