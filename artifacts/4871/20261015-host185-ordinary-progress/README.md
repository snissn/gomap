# Host185 ordinary repair checkpoint

This is a bounded repair and infrastructure receipt, not final qualification or mergeability evidence.

Frozen ordinary source: `0046314c626310f7ad7965360155335a80cdc459` (tree `0aafe0c30b6897b52ac9483f8b9cd4f2f803c7a1`), pushed on `codex/5111-host185-early-cost-20261015`. It contains the 15-file repair packet over transferred ordinary source `f3180bb8eb8e53448a780cc743f1f4b16a3fa8bb`. Later fixture repairs remain in the ordinary lane and are not represented as this source.

The unchanged eight column allocation guards pass. Snapshot-owned inline cache storage removes the measured extra backing-slice allocation while retaining the original public snapshot and iterator ownership. The cached CompactStorage path now refreshes its old COW read basis under the existing maintenance writer fence before retrying blocked leaf retirement. Normal reader and recoverable-root protections remain active, and settlement does not checkpoint or publish a root.

`column-all8-current-07` and `settle-lifetime-06` pass with all recorded Go source bytes matching the frozen candidate. `public-failure-controls-08` is deliberately retained as RED: 12 of 14 original controls passed, including public shrink/reopen, while page-reuse and meta-closure fixtures still failed at this checkpoint. `cwal-settle-05` passes the original CWAL/no-WAL deletion/reopen controls; its production bytes match the candidate, but its caching lifetime test file predates the added settlement test. Full source hashes before and after every command and a compact comparison are retained. No run in this packet had source drift during execution.

Both baseline `5bca87ffe6e1bfba2d693264d2dc82936adc234f` and candidate benchmark binaries built with pinned Go1.26.3. The same public PRIMARY-after-DATA generation harness is used through a baseline Go overlay. The candidate public lifecycle control passes with true DATA seed activation, no PRIMARY generation advance, and reopen. These are build/correctness results only. The initial baseline build cache miss and subsequent successful build are retained separately.

**No benchmark samples were collected on host185.** The first strict preflight rejected active pulseaudio and ClickHouse CPU use. A separately labeled background diagnostic also rejected additional polystorechaind and htop activity before any benchmark began. Both original runner byte sequences are archived and match their receipt hashes. No existing service was stopped or reconfigured. Local timing remains unavailable until an actual host-state change; the existing hosted PR raw-path gate is the next performance route.

INFRASTRUCTURE_UNAVAILABLE: runner: no quiet dedicated local runner was established on host185. Task-local build caches and durable local/Git artifacts are available. Hosted CI has not run for this early checkpoint and is not implied by these local results.

The accepted compatible native closure decision is retained here and published at https://github.com/snissn/gomap/issues/4878#issuecomment-6196342249. Native implementation is ongoing in its separate source lane. Actual producer/C1/C3 integration, original Q32/ACK0, full ordinary nineteen-group coverage, platform gates, current PR CI, independent review, and final graph qualification remain open. This artifact changes no acceptance cap, workload ratio, or merge gate.
