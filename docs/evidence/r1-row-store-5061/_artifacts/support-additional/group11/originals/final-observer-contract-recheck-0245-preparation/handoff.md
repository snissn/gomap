# Final C/A/D observer contract recheck

Preparation only, against candidate `0245d8e0f5835dcba2929e4f1ba8b5d7ea909133`. No actual merge, remote observation, Go command, capture, CI polling or GitHub action occurred. Originals remain unchanged. Observer/CLI/static source pins are in `handoff.json`.

No static flag/compiler-invocation mismatch was found. The root must freeze the **actual** selected merge SHA M after landing, not0245/prelanding02ca/2572/f796. Use the clean full remote checkout `/home/mikers/gomap-r1-final-source-20261006` only after independently confirming it has M. Put configs, observers, receipts and outputs outside that checkout.

## Config recommendations

All three configs should set `repo` to the clean full remote path, `source_commit` and `landed_tooling_commit` to M, `review_url` and `landing_observation` from actual external reviewed/landed observations, and independently derived runtime/harness hashes. The D validator permits an ancestor landing, but selecting M for both fields is unambiguous for this planned final run. A/C explicitly require equality. Do not fill expected values from packet metadata.

Shared111 settings: `real_go=/home/mikers/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.4.linux-amd64/bin/go`, `goroot` its parent, `gomodcache=/home/mikers/go/pkg/mod`, a root-selected matching Go1.26.4 cache and `lock=/home/mikers/gomap-r1-evidence-20261005/timed-capture.lock`. Observers freeze16/100/off, empty GODEBUG/GOFLAGS, GOENV/GOWORK off and GOTOOLCHAIN local. Actual compiler/CGO/toolchain/environment remain observable, not copied baseline values.

A/C `scripts/r1_collection_source.py` derives the same committed runtime manifest and actual command-harness byte digest. D `r1_lifecycle_capture.source(real_go)` shares runtime identity but hashes the actual Linux compiled collection test files and lifecycle helpers; its harness hash is DIFFERENT. Derive it anew under matching environment from M, using the reviewed source helper. This entails `go list` and belongs to the later authorized root action, not this audit. Candidate original A/C values runtime`cb5b0d7b3633666ae16752d8c353eeeb23542c71e5e25773e33359d3ffa79aa9` and harness`706b71f11508bf9c0075b0dd9c5463cd741a49cba83996feaf753330ace77822` are candidate reference observations only, not final expected values.

For each lane select a **nonexistent**, distinct output and receipt path, e.g. under `/home/mikers/gomap-r1-evidence-20261005/final-M-{C,A,D}` and root-private `/home/mikers/gomap-r1-final-receipts-20261006/M-{C,A,D}`. Do not precreate these leaf directories. Their parents must exist; A's runner observation stats/df's the output parent before capture. Keep configs in a third existing private directory, outside source/output/receipt trees. D benchmark TMPDIR is created under its output; A/C temporary databases are under receipts. Never reuse failed directories.

## Exact future observer commands

The following assumes root copied and hash-verified the reviewed private observer files into `/home/mikers/gomap-r1-final-observers-20261006/` and wrote completed configs outside the source. These remote destinations are recommendations, not paths observed by this audit. Run serially after root's landing/config authorization; each command holds the actual canonical111 flock. `R1_RECEIPT_LOCKED` alone is only a marker, not proof of a held lock. Prefixing PYTHONDONTWRITEBYTECODE at interpreter startup also prevents C/D import bytecode writes during source inspection.

```sh
flock /home/mikers/gomap-r1-evidence-20261005/timed-capture.lock \
  env PYTHONDONTWRITEBYTECODE=1 R1_RECEIPT_LOCKED=yes \
  python3 /home/mikers/gomap-r1-final-observers-20261006/observe-C.py \
  /home/mikers/gomap-r1-final-configs-20261006/C-M.json

flock /home/mikers/gomap-r1-evidence-20261005/timed-capture.lock \
  env PYTHONDONTWRITEBYTECODE=1 R1_RECEIPT_LOCKED=yes \
  python3 /home/mikers/gomap-r1-final-observers-20261006/observe-A-with-environment.py \
  /home/mikers/gomap-r1-final-configs-20261006/A-M.json

flock /home/mikers/gomap-r1-evidence-20261005/timed-capture.lock \
  env PYTHONDONTWRITEBYTECODE=1 R1_RECEIPT_LOCKED=yes \
  python3 /home/mikers/gomap-r1-final-observers-20261006/observe-D.py \
  /home/mikers/gomap-r1-final-configs-20261006/D-M.json
```

Copy C from `mutation-receipt-preparation/observe.py` SHA`ebe05a3822f2716596831ceb464bc8c95adb340eb52f7bfa75b87183a3602efa`; A from `final-A-semantic-equivalence-preparation/observe-with-environment.py` SHA`f477dacf465f347afa423759cbc6f148f8e6bbb391903e333bcddc0cd323990d`; D from `retained-receipt-preparation/observe.py` SHA`03c9807292fb42a7c09fcbafed4895703a5477a92e7f827331b93d9084282c68`. Do not use older A observer without actual runner metadata.

## Actual driver and validation contracts

| Lane | Observer's actual capture arguments | Build | Independent validation |
| --- | --- | --- | --- |
| C | R1_MODE=r1-mutation-sweep; `-qualification retained -documents4096 -operations100 -repetitions5` (separate argv tokens) | `go build -o OUT/collection_workload_bench ./cmd/collection_workload_bench` | `r1-mutation-sweep-validate -source-manifest RECEIPT/independent-expected-source.json` plus six separate receipt pins and original packet path |
| A | R1_MODE=r1; `-qualification retained -documents4096 -batch-size32 -operations1000 -repetitions5 -durability durable -read-state flushed -engines json,template-v1,bson,typed-row,sqlite-json,sqlite-row` (separate tokens) | same command build | original `r1-validate -source-manifest RECEIPT/independent-expected-source.json OUT/packet.json`; observer checks independently frozen executable/packet/source bytes before and after |
| D | `--qualification retained --out OUT --repetitions5 --epochs5 --documents4096 --calls-per-epoch1024` plus external source/runtime/harness/landed/review fields (separate tokens) | `go test -c -o OUT/collections.test ./TreeDB/collections` | `python3 scripts/r1_lifecycle_validate.py OUT/packet.json` plus six separate receipt pins |

These compact flag/count spellings in the table describe values, not runnable joined flags. The observers already produce the correct separate argv tokens. C's producer semantic-only validation and D's driver selfcheck are explicitly unqualified; final acceptance uses separate root receipt hashes. C original driver has no A summary.json. Root must invoke its reviewed16-group sweep summarizer after independent validation. A root report helper is descriptive only. D summary reports finite512-ID hot-set/4096-live-row/5-epoch scope, actual maintenance and component trajectories, without automatic capacity acceptance.

A matched comparison must run111, matching original baseline host/device/compiler/runtime identity;185 is not a drop-in comparable runner. Use actual pre/build/post runner observations and the approved semantic-equivalence certificate, not baseline-derived expected environment. The four baseline A blobs are still byte-identical at0245 in this static check. This is provisional eligibility only: actual M blob/inventory/diff review, independently observed source/binary/packet receipts, accepted freeze and approved certificate remain required. Retain all original0216 files and old candidate packets unchanged.

Read-only lane released. Root owns actual merge, configurations, capture, independent acceptance and publication.
