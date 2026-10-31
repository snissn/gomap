# Race control: incomplete, resource guard stopped build

The one authorized race build was terminated by the unchanged filesystem-floor guard after 42.92 seconds. Child exit was SIGTERM (-15); guard exit was 143. Stop reason: `filesystem_floor_reserve`. No race ELF was produced, no race test executed, and no retry or correction occurred. This is neither a race PASS nor a detected data race.

Source remained `e388a86d51df6339af6ce55e1170d7a44970ca125dfa75ed007b9fa558ac40bf` on base `95c6ff3de473786bd98b692375cfdb9cef951668`. No source edits, commits, pushes or GitHub writes occurred. The sealed normal evidence manifest is unchanged. Its earlier six-test normal PASS remains scoped diagnostic evidence.

The pinned Go 1.26.10 GOROOT, GOTOOLDIR and compiler agreed. The command used normal optimization with `-race -p=1 -gcflags=all=-c=1 -mod=readonly -trimpath -buildvcs=false`, offline settings and new task-local `cache/held-sync-race` and `tmp/held-sync-race`. Exact command, environment, source/toolchain/guard hashes and child PID are in `race-preflight.json` and `race-build-1.command.json`/`.result.json`.

Guard floor: 8,589,934,592 bytes plus a 67,108,864-byte stopping reserve. Minimum sampled free space: 8,643,624,960 bytes; final free space: 8,643,620,864 bytes, preserving the floor by 53,686,272 bytes. Peak execution packet allocation: 591,060,992 bytes, below the 2 GiB cap. The guard sampled every 0.2 seconds plus packet-scan time, with a 20-minute child deadline. No guard policy was changed. Free space fell by about 936 MB while packet allocation increased by about 458 MB; the additional decline is unattributed outside-packet filesystem activity, not a measured property of this control.

All build/test/guard processes have ended. Runtime, cache and temporary-path ownership is released to the root coordinator; no consumers remain. Partial task cache/temp are retained for the coordinator's audited retirement. No binary hash/build metadata can exist because compilation did not finish. Raw build log, resource samples and exit receipt are retained, including the empty build log if no compiler diagnostic was emitted.

Remaining: coordinator disposition of the incomplete race gate and the diagnostic prototype; source quarantine and a docs-only decision are coordinator-owned. The optional public diagnostic remains unlaunched. No further build/runtime action is authorized for this worker.
