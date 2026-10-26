# Shared text selected-sidecar allocation comparison (#5245)

Measured source: `171737eb194961c7183fec471440cb761e51dcf3`. Baseline production anchor: `a621e9a4ac0904dfe012b9c9e101954cebd503d0`. This evidence selects the shared decoded-sidecar retention change under the predeclared finite rule. Full owning-package, CI and final review gates remain pending.

All eight rows from the four fixed processes were valid. For **each** query the rule required median B/op to decrease at least 20%, median ns/op to increase no more than 5%, and median allocs/op to increase by at most one. Keyword decreased 59.94865% in B/op; Hybrid decreased 50.08238%. Objects were unchanged. Observed time changes were −3.32673% and −2.82534%; two samples per arm do not support a statistical or universal timing-improvement claim.

## Workload and applicability

Both arms use the unchanged landed public document-service benchmark with the **legacy cached owner**, resolved `column_graph` vector strategy, `command_wal_durable`, 4096 rows loaded through 128-document requests, 16 tenants, Top10 and projection assertions. The benchmark omits strategy and reports its actual resolution. Although it requests `cow_btree`, the backend-only cached opener overrides that request to `btree`. These are shared text results: no native/COW activation, storage-default qualification, command transport throughput, or durable query ACK is established.

Setup, warmup, ordinary Optimize and cleanup lie outside the query timer; their failures still invalidate the process. Each selected subbenchmark executes 1000 operations at GOMAXPROCS=4. Keyword scans 640 postings/op; Hybrid scans 1024 postings/op and fetches 10 documents/op, identically in all samples. Existing assertions check tenant/projection and no full-document-scan fallback. B/op and allocs/op measure Go benchmark allocation churn, not live/peak memory or process RSS.

## Observations

Medians are computed per arm from its two retained rows. Derived ops/s is `1e9 / median ns/op`, not transport throughput.

| Query | Arm | Median ns/op | Derived ops/s | Median B/op | Median allocs/op |
| --- | --- | ---: | ---: | ---: | ---: |
| keyword | baseline | 684,201.0 | 1,461.56 | 681,627.5 | 2,301 |
| keyword | candidate | 661,439.5 | 1,511.85 | 273,001.0 | 2,301 |
| hybrid | baseline | 699,597.5 | 1,429.39 | 816,021.5 | 2,317 |
| hybrid | candidate | 679,831.5 | 1,470.95 | 407,338.5 | 2,317 |

Every sample, in frozen process order:

| Process | Query | Iterations | ns/op | Derived ops/s | B/op | allocs/op | Postings/op | Docs fetched/op |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| baseline1 | keyword | 1000 | 685,398 | 1,459.01 | 681,613 | 2,301 | 640 | — |
| baseline1 | hybrid | 1000 | 701,397 | 1,425.73 | 816,022 | 2,317 | 1,024 | 10.0 |
| candidate1 | keyword | 1000 | 646,288 | 1,547.30 | 272,989 | 2,301 | 640 | — |
| candidate1 | hybrid | 1000 | 695,306 | 1,438.22 | 407,352 | 2,317 | 1,024 | 10.0 |
| candidate2 | keyword | 1000 | 676,591 | 1,478.00 | 273,013 | 2,301 | 640 | — |
| candidate2 | hybrid | 1000 | 664,357 | 1,505.21 | 407,325 | 2,317 | 1,024 | 10.0 |
| baseline2 | keyword | 1000 | 683,004 | 1,464.12 | 681,642 | 2,301 | 640 | — |
| baseline2 | hybrid | 1000 | 697,798 | 1,433.08 | 816,021 | 2,317 | 1,024 | 10.0 |

## Custody and reproduction

The frozen plan SHA256 is `2773d5e5a54f2e87ed9c8c1081fd10f2b3ca55c49693defa897b858880752c66`. Fixed order: `baseline1 → candidate1 → candidate2 → baseline2`; no retries. Both executables were built before timed execution. Baseline is the measured source with only `text_v2_search.go` and `text_v2_phrase.go` restored from a621 by the archived Go overlay. The remaining source delta consists of the new representation test, contract documentation and CI inventory metadata; the landed harness is byte-identical in both arms.

- Baseline ELF SHA256: `360aafc25e0960ebed69ef0554fca990d1dd7ae7a830de0cb4da7e60929e2941`.
- Candidate ELF SHA256: `acf9e88e29fea06f46fbb74a48661504be976de8be0a8b94ed157e51393ed7ac`.
- Landed harness SHA256: `cc27ffef2e22d1e9c5aa0dc34c79990e6d204d02b27d555b1b8b7ec1357822a7`.
- Go 1.26.0 driver SHA256: `61e7455a40a2fdfcdab99e881cd30ba10e216e3d0f32ab5f8e59d10cac4ecf57`.

Original exact commands, build flags, frozen environment construction, source/tree/overlay checks, timestamps, raw stdout and process-group cleanup are in the archived plan, runner, helper and receipts. Linux/amd64 host: Intel Core i5-11400F @ 2.60GHz; shared host without dedicated hardware reservation. The environment selects GOMAXPROCS=4, GOWORK=off, empty GOFLAGS, pinned absolute toolchain with GOTOOLCHAIN=local, task GOCACHE/TMPDIR, legacy backend and durable profile. The scripts retain their original absolute paths as provenance; replay requires reconstructing those paths/bindings or a newly frozen plan. Publication did not rebuild or execute any code. Executable bytes are not embedded; their preserved identities were rechecked against the original binaries.

## Correctness and review evidence

The archive includes the original independent source ACCEPT, source audit, complete patch/snapshots, test-first representation exception, and these exact successful root-owned controls. The receipts bind the original a621 worktree plus the same four candidate file hashes later committed at 171737e; this publication does not relabel their original source state.

| Check | Selector | Package seconds | Process wall seconds |
| --- | --- | ---: | ---: |
| focused1 | `^(TestTextV2SelectedSidecar.*|TestTextV2ScalarPruningPostFilterParity2874|TestTextV2ScalarPruningMissingAllowedDocIDFailsClosed2874|TestTextV2ScalarPruningHybridReopenNoDocGuardrail2874|TestTextV2BlockMaxRandomizedExactnessAndFallbacks2628|TestTextV2HardeningCorruptionFailsClosedAcrossShapes2734|TestTextV2SearchSnapshotBindingAndReopen2627|TestTextV2PhrasePositionsReopenAndSnapshotBinding2733|TestTextV2PhraseAnalyzerStopwordsAndScalarAllowSet2733)$` | 0.723 | 45.797 |
| text-owning1 | `^TestText(V1|V2)` | 7.912 | 9.179 |
| race1 | `^(TestTextV2SelectedSidecar.*|TestTextV2ScalarPruningPostFilterParity2874|TestTextV2ScalarPruningMissingAllowedDocIDFailsClosed2874|TestTextV2ScalarPruningHybridReopenNoDocGuardrail2874|TestTextV2BlockMaxRandomizedExactnessAndFallbacks2628|TestTextV2HardeningCorruptionFailsClosedAcrossShapes2734|TestTextV2SearchSnapshotBindingAndReopen2627|TestTextV2PhrasePositionsReopenAndSnapshotBinding2733|TestTextV2PhraseAnalyzerStopwordsAndScalarAllowSet2733)$` | 1.929 | 84.513 |
| naming1 | `^TestTypedStorage` | 12.308 | 13.534 |

The representation-only optimization has no claimed public behavioral RED: its new private representation assertions require the new symbols. The documented exception relies on public activation/cost evidence and existing semantic/corruption/capture/ownership controls, rather than calling an old-source compilation failure a regression.

## Files and verification

- [manifest.json](manifest.json) records every archive member SHA256/byte count/original location, exact source, harness/toolchain/ELF identities and validation boundaries.
- [raw-evidence.tar.gz](raw-evidence.tar.gz) contains all raw eight rows in four original logs, six process/build receipts, frozen plan/script/overlay/outcomes, runner dependency, original source-review/test-first artifacts, four validation logs/plans/receipts, and measured source/harness snapshots.
- [SHA256SUMS](SHA256SUMS) covers the published README, manifest and archive; manifest member checksums verify extracted original bytes.

Only this evidence directory is added after measured source 171737e. A later evidence-only commit may reuse these rows only with unchanged production, harness, toolchain and workload applicability demonstrated; it must retain 171737e as the measured identity. No native/default qualification or completion of #5225/#5238/#5241 follows from this narrow acceptance.
