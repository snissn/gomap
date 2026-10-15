# Allocation construction final freeze

Final source commit: 7a6fd35f56dc6ddb70ae76e91c36656e7055a8df, parent ordinary
177fb0c8db1d19c79d3d975126b4b0b64019568e. Exact19-file composition manifest SHA
5a06010306af8c9120a5124902b4e25b250c0a1db3618efd58cdb239a30bd7b3.
public-final-source/committed-source-binding.json proves all measured Go inputs
still equal committed source; commit changed no compiler input. Binary SHA
b69ce95bde5b32d6ceb73274b179565ebcbe39b48d0dd5aa0f2c8f0d00d73647.

Final-source full freelist suite PASS (0.133s), including public snapshot work
admission and public candidate page amplification; focused seven private-authority
plus snapshot tests race PASS (1.008s). Exact commands/env/logs in public-final-source.
Prior DB format/reopen GREEN remains separately source-bound to unchanged DB code.

Three alternating-order fixed1000x paired public BatchWrite measurements on CPU0,
GOMAXPROCS1, Go1.26.3, same10000-key seed and stopped-timer checkpoint every8 writes:

| Median | ordinary177 | final | Change |
|---|---:|---:|---:|
| B/op | 154838 | 45181 | -70.82% |
| allocs/op | 286 | 144 | -49.65% |
| ns/op | 46678 | 23542 | -49.57% |

Every pair improves latency. These short construction diagnostics do not replace
remaining graph-wide qualification, current-head CI, review, or final acceptance.
No setup/ACK/maintenance boundary or fixture size changed; no zipper patch used.

Matched1000x Rate1 final/ordinary profiles separately observe exact public Write
stacks (1001 writes including warmup), excluding seed WriteSync and Checkpoint.
Named flat page+reservation-digest union: encodeIndexPage, encodeChunkPage,
encodeNormalizedReservationPages, encodeGenerationPage, reservationDigest.
87462120 to16457352 allocated bytes (-81.18%). Includes all allocating Into
variants if present (none appear here). Index+chunk subset75137024 to8232960 B
(-89.04%). These are disjoint flat allocation sites; no cumulative frame sums.
Original ticket57.23% denotes a stack-union attribution and is not numerically
recomputed by this flat-site metric. Exact rows and definitions are in
public-final-source/flat-attribution.json. Public Write allocation totals:
154879392 to45119128 B; pprof percentage denominators are whole-process
190844824/59958616 B and must not be used as Write-only denominators.

Private incremental mechanism was measured separately, on the earlier integrated
source: fixed10000x medians B/op-19.42%, allocations-7.69%, latency-8.01%; flat
clone+mandatory-detach bytes-66.41%. Preserve that historical binding in
public-private-incremental. Since then private code only changed formatting;
snapshot work-classifier P1 and its test changed separately. Final ordinary-vs-
combined profiles freshly bind final source and show identical private clone flat
bytes5468048 B to the earlier combined observer. Do not relabel the historical
counterfactual as final-source profiling or add percentages from these comparisons.
No retained-heap qualification was performed by this lane.

Original measured counterfactual: topology-only-overlay (exact historical variant,
manifest/diffs and compiler overlay). Current source reproduction variant:
topology-only-overlay-final, explicitly UNMEASURED, regenerated from final source.
Both remove BeginCandidate detach/grant and ordinary prepare uses plain clone;
mandatory materializer detach stays once. Neither variant is a correctness
candidate; private activation tests are expected RED. Product source unchanged.

All runners/harness binding, argv/env, raw measurements, source census, binary SHA,
profiles and overlays remain in this private evidence directory. handoff.md records
implementation/ownership controls, representation RED and retained-fixture rationale.
Serial runtime released; no owned child remains. Evidence writing complete.
No source, worktree, binary, profile, overlay or evidence path is released for
deletion. Git/GitHub source/artifact publication and final gates remain root-owned.
