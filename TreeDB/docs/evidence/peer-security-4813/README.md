# P7 authenticated transport/admission evidence

This packet records local/conformance evidence, not EC2 scale acceptance.
Generic operations are still subject to current-head CI/review/merge; retained
historical results must not replace those gates. #4250 owns real multi-host
campaigns and #3983 owns distributed fault evidence.

## Actual red/green boundaries

| Behavior | Actual red job/head | Green evidence |
| --- | --- | --- |
| Idle endpoint growth | [106819474466](https://github.com/snissn/gomap/actions/runs/35749454831/job/106819474466): 96 real idle endpoints retained 96 sockets against a 64-socket budget | Exact-head TCP repeated/race cases in qualification |
| Unknown control/Raft peer | [106832578325](https://github.com/snissn/gomap/actions/runs/35753270982/job/106832578325): uncredentialed durable commit and snapshot RPC | TLS identity/group denial tests through real listeners |
| Too-small request cap regresses existing M8 | [106832578031](https://github.com/snissn/gomap/actions/runs/35753270982/job/106832578031): P1 base passes, 64-slot candidate fails | Restored bounded 256-slot dispatcher plus per-group reservations; paired [106834665766](https://github.com/snissn/gomap/actions/runs/35753887693/job/106834665766) passes base/head |
| Plaintext native/shard boundary | [106837744559](https://github.com/snissn/gomap/actions/runs/35754805313/job/106837744559) | Shared authenticated transport across both real protocols |
| Stalled snapshot monopolizes a group | [106846207523](https://github.com/snissn/gomap/actions/runs/35757307358/job/106846207523) | [106850296786](https://github.com/snissn/gomap/actions/runs/35758512918/job/106850296786): repeated/race stalled-snapshot test green |
| Raft decode waits on declared unbudgeted payload | [106850296786](https://github.com/snissn/gomap/actions/runs/35758512918/job/106850296786) | [106856578715](https://github.com/snissn/gomap/actions/runs/35760358222/job/106856578715): bounded predecoder and outbound/pipeline admission, repeated/race |
| Loss of one persistent root silently recreates state | [106859362904](https://github.com/snissn/gomap/actions/runs/35761199192/job/106859362904), head `10f5e56b1f541d442e8471b522077061feccdba2`, both Data/Raft loss repeated and under race | `666461a7e4d9111b1d4661de14a3bfa988609a58`: both focused qualification jobs green, including paired-root refusal |

The first local replicated TLS shutdown test also found an actual upstream
pipeline consumer panic when the wrapper closed its consumer channel. The wrapper
now preserves upstream shutdown semantics, completes futures exactly once and
keeps the replicated test under the race detector in hosted qualification.
A single upstream future's non-concurrent `Error` path is consumed by one pump;
callers observe the wrapper's own completion event.

## Matched enabled-path cost

Raw numeric records are in
[`measurements-669a062.jsonl`](measurements-669a062.jsonl). They come from
exact source `669a06220c08aa7d0d0d90830105fe19a02dad34`, public-path-cost job
[106856578488](https://github.com/snissn/gomap/actions/runs/35760358222/job/106856578488),
Go 1.26.8, Linux/amd64, the same runner, four actual processes, three repeats of
25 fresh durable remote-owner creates per mode/cell. The source/toolchain/harness
hashes and per-child observations are in that job's uploaded artifact. Secure and
plain modes use the same actual API, route/ownership proof, quorum/apply and fresh
idempotency semantics. Startup is excluded; a separate route measurement retains
the quorum-fence cost. Inventory40 adds 36 dormant identities, not 36 live nodes.

| Cell | Plain median | TLS/admission median | Enabled increment |
| --- | ---: | ---: | ---: |
| Sparse | 6.516536 ms | 6.942621 ms | +6.54% |
| Inventory40 | 6.340991 ms | 6.872949 ms | +8.39% |

Plain parent allocations were about 83–85 KB / 488 allocs per write; TLS about
84–86 KB / 489–491. Aggregate child allocations were about 1.24 MB / 4,972–4,994
versus 1.26–1.27 MB / 5,206–5,243. Both modes retained 112–114 aggregate FDs.
Plain/TLS aggregate goroutines were 96/105; aggregate sampled RSS was about
181–188/190–193 MB. The JSONL retains every repeat, route cost, child heap/GC and
summed individual RSS high-water marks. Summed individual peaks are not a
simultaneous aggregate peak. Small samples include background Raft/metrics work;
these are honest local enabled-path overheads, not a speedup or throughput ceiling.
No claim about large snapshot catch-up or high-concurrency secure throughput is
made from these fresh-collection operations.

Earlier fixed-head enabled measurements at `574263c...` showed +2.78%/+3.38%
before the full pre-decode/pipeline byte accounting was installed. They are not
substitutes for the later full-boundary cost. Final operations/IP-counter changes
must retain their own current-head measurements before merge.

## Operations-head confirmation

Exact operations source `f13699dff6ebfc1bbd8243e696394f7fecc4f14d` passed
[public cost job106868617427](https://github.com/snissn/gomap/actions/runs/35763949327/job/106868617427)
and [security/operations qualify106868616735](https://github.com/snissn/gomap/actions/runs/35763949327/job/106868616735).
All 12 records are in [`measurements-f13699d.jsonl`](measurements-f13699d.jsonl).
Sparse plain/TLS medians were 6.508654/6.690200 ms (+2.79%); Inventory40
6.331073/6.725239 ms (+6.23%). Allocations/retained process resources remain in
the same measured range as the prior full-boundary source. The difference between
these short runs is not evidence of a speedup; both records are retained.

The separate P1 qualification
[job106868623042](https://github.com/snissn/gomap/actions/runs/35763949196/job/106868623042)
failed a snapshot-restart route assertion with `catalog meta unavailable` after
its fixture waited only for observational `State=Leader`. Thirty local repetitions
passed (32.494s), so no deterministic local reproduction or production regression
is claimed. The fixture now waits for actual catalog/data quorum/apply readiness
**after reopen**, before its unchanged one-shot snapshot/version/mutation/stale
assertions. It retries only these precondition reads; mutation retries are not
introduced. The stronger fixture passed three repetitions under race (4.303s)
and must pass the subsequent exact-head hosted gate before merge.

Independent review of `f13699d` found that readiness always attempted a local
catalog fence, making catalog consumers permanently unready. A new real TLS
four-node sparse test reproduced the failure for both a storage-free consumer and
a data-owning consumer: `Live=true Ready=false CatalogEpoch=0` with
`catalog meta unavailable`, despite initialized remote quorum and durable data
commands (1.539s). The fix uses the existing request-scoped catalog leader read
for consumers, without installing/caching local authority. Both sparse variants
also check unrelated data-group outage, loss of catalog quorum, and no invented
local stores/authority. Combined with the stronger snapshot fixture, three local
race repetitions passed in 11.606s. The exact reviewed finding is
[4074786847](https://github.com/snissn/gomap/pull/4817#discussion_r4074786847);
subsequent head CI and independent review remain required.

## Allocation/lifetime audit

- Startup: one bounded normalized configuration clone/hash, group/identity/IP
  tables, credential parsing and a fixed scope-reservation ledger. No peer mesh
  is preconnected. Redundant transport validation/cloning was removed. Paired-root
  markers reuse the already computed normalized config bytes.
- Per request: acquire fixed global/per-scope request and byte leases before
  body reads/decoding/network send. Avoid marshaling an oversized control request;
  native/shard response writers refuse while encoding, before exceeding their
  configured frame bound. Fixed peer identity/endpoint lookup does not scan the
  inventory. Existing command/storage allocations remain separately owned.
- Per Raft connection: reserve bounded bufio/codec/response capacity through
  active and idle lifetime. The framing scanner charges bounded chunks before
  exposing untrusted MessagePack container/payload lengths to HashiCorp decoding.
  One-entry secure append batches and bounded pipelines retain leases to actual
  future completion. Snapshot streams retain admission through installation.
- Diagnostics: only explicit/periodic operator requests snapshot the fixed IP
  inventory and process/host counters. There is no per-request all-inventory
  metrics allocation or per-interval recursive dataset walk. Unknown IP traffic
  cannot grow the table. Counter sampling is not an atomic distributed snapshot.
- Shutdown/reconnect: close/cancellation releases sockets and pending dials;
  leases are released on failure/completion/close. Borrowed control clients own
  only their pools. No mutation auto-retry, credential downgrade or proof cache.

The shared admission byte budget is not a whole-engine heap bound. Kernel socket
buffers, user-owned values and storage working sets are outside it. P2's bounded
source/publication work and later real corpus/RSS qualification cannot be waived
by these transport measurements.
