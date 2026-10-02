# F4895 baseline full-freeze preparation review

The local baseline bridge is ready for independent source review. One new bounded Mac preparation compiled the canonical H/T workload through baseline-only test overlays with `pilot=false`; no benchmark was executed. This is construction evidence, not retained performance evidence or acceptance. The original tracked product remains exactly `ef22ce55b85524de2707453f22c5f8cd6ba89eaa`.

## Scope and source identity

Owned proposal: `/tmp/gomap-4895-baseline-compat-proposal`. Owned detached product checkout: `/tmp/gomap-4895-baseline-compat`. The dirty primary and H/T/A/N/V worktrees were not changed. No commit, push, GitHub operation, Linux operation, retained run, or delegation occurred. There are no commands left running.

Canonical H is explicitly `7e1c51d41bcc0392fcb17dec12e46fdab42449bc`, from `/tmp/gomap-issue-4894-harness`. The shared T/A stage `1d4826dda8ae619cf95d810c53d6bd5fe10a8c3a` does **not** contain H. H is separate provisional instrumentation and is not represented as landed by this freeze. The later retained freeze must bind the actual reviewed and landed H source through current hashes.

Original product tracked tree: `c023d39fee5b91e3764b7669c415b4182df429b3`. Original product TreeDB tree: `d3dcef28f252494cf083b8058ee9e06779949842`. Maintenance-control product `1f0b09b8ffad0c3eaa1eae0146958f45e57d8c1c` is an explicitly supported identity in the source guard, but it was not constructed or executed in this continuation; it requires its own reviewed tree/addition/tool manifest and freeze.

Canonical Go source SHA256 values:

- `TreeDB/quicksilver_workflow_bench_test.go`: `05d2a485efd14d43e3f60f715761e3cbfb50b7a31aa11e55f44e8d4ffc42da96`
- `TreeDB/memory_budget_bench_test.go`: `03b389e475374af7157111b4a8feea72a22124aa4fec5f3342d512ea6cbad462`

The bridge preserves the fixture, query sequence, CRC checks, phase timing, placement, reopen, and LSN behavior. The baseline lacks `Options.NegativeLookupFilterBytes` and all `treedb.negative_lookup_filter.*` stats. The amended tests reject filter-on before opening, and the baseline-specific validator requires complete absence of that stats prefix at every boundary. No engine-zero observations are invented. The packet has its distinct `quicksilver-baseline-workflow-v1` schema and `negative_filter_support: absent`. The small missing `algorithmQuantile` helper is copied from the existing H7e1 A test, as documented in the original compatibility review. There is no production backport.

## Small explicit preparation path

The new `prepare_baseline.py` wraps the existing memory-budget overlay generator and capture preparation. It generates the existing test-only cache-budget setup overlay and maps the two canonical Go test paths to the reviewed baseline-compatible replacements. It compiles a test binary and stops.

The existing full preparation clean-check is unchanged for normal callers. The explicit baseline mode requires an externally supplied SHA256 of `baseline-full-source-manifest.json`. It checks exact product HEAD and both tree identities, rejects tracked edits including staged edits, and requires exactly these five untracked additions, with exact per-file hashes:

- `TreeDB/memory_budget_bench_test.go`
- `TreeDB/quicksilver_workflow_bench_test.go`
- `scripts/treedb_memory_budget_capture.py`
- `scripts/treedb_memory_budget_overlay.py`
- `scripts/treedb_quicksilver_capture.py`

All five additions must be regular files, not symlinks. The two Go additions must match the hard-pinned canonical H7e1 hashes. Any other nonignored untracked file is rejected; ignored source files with the capture helper's existing `SOURCE_SUFFIXES` are also rejected. The guard does not arbitrarily waive dirty checkout checks.

The five additions retain their historical first-pilot bytes. The new amended Python helpers execute externally from the proposal directory. The manifest separately pins the exact external H/T replacement sources, three Python capture/overlay inputs, preparation helper, and actual compiler executable. The executing memory and quicksilver capture paths and preparation path must agree with their external declarations. The guard is run before/after compilation and again whenever a freeze is loaded; every retained run using the existing capture path verifies the freeze before/after execution.

The existing capture implementation also freezes the actual overlay files, replacement source inputs, module manifests encountered by `go list`, compiler/toolchain inputs, environment, binary, process argv/exit status, and stdout/stderr hashes. Here 4,417 compiler inputs were hashed. `inputs.stdout` and `inputs-after.stdout` have identical SHA256 `12fbf12a98c6dda9671b731fb0679ec57108a3e241a1bd95cb0b3cebfc53572e`; the complete list is in the freeze. The recorded module input is baseline `go.mod` SHA256 `a5ee500cad24e42459629bc5027fafb1b106996b0639473c920d19a2062e7d6f`. No dependencies were added.

## Reviewable artifacts and hashes

All paths below are inside `/tmp/gomap-4895-baseline-compat-proposal`:

| Artifact | SHA256 |
| --- | --- |
| `baseline-full-freeze.patch` | `87b6f7dd33a3eae4d52bb06716c1b4263174e6b5dff16b6bbe82ce502f43c728` |
| `baseline-full-source-manifest.json` | `1a4475df41464a9619e4ad9ca494d0e8d96e183f26980fbea3f4e586dc0b9765` |
| `full-source-hashes.json` | `52b973bbd3b5153db67b3ee840aa6797d681b51356a406f4cf5896c7831cbf94` |
| `full-preparation-proof.json` | `680301e818e5d39edac59577b5d6b8ac17132352ff063957c772440e3845b096` |
| `prepare_baseline.py` | `573c0e069e8c86babc2052a3c7064358f0eb1d9f66fe662a1df7f7e5d4bb1655` |
| `check_full_freeze.py` | `b1887f88cfdcc10fbb7107d63c0596b936d17d4f0de7bbbff163d283eea6a58e` |
| `scripts/treedb_memory_budget_capture.py` | `21a530d7b8addff48ff2280f9949249d61ce25f743378ab4f022afcb45da728a` |
| `scripts/treedb_quicksilver_capture.py` | `4873183be0d55ae945987cbae42197a3f6730460061d5b7092d1a9ae1a0ea428` |
| `scripts/treedb_memory_budget_overlay.py` | `156abcee681ea4de1366df75e6dfe868ead23a240334f33d331804f529730167` |
| `TreeDB/memory_budget_bench_test.go` | `68599f74579bcb0f5dba0155a70120ec2a480185044f0edc09cca930191e4508` |
| `TreeDB/quicksilver_workflow_bench_test.go` | `84d0c67576d3e3396baa5c421bcf6ac6158958787e071d45e808337d148c3736` |
| `full-prepared/freeze.json` | `35194ed0ad688d104b1760769f64d3c0d5788218c34bf5f0628ffb4fa6183e3c` |
| `full-prepared/memory-budget.test` | `56275e8b45b867445b03c9434889d6a1dad862cd1d2c25209f2f751ed5c0a633` |
| `full-prepared/overlay/overlay.json` | `6bc66019b45ebfef148a055a3ad2fa6a3cd9804e5a8563d1831d9ffad6813ecd` |
| `full-prepared/overlay/identity.json` | `4800b8bbe414c7d6e06814d50600731fc87311508cb0929dce8245e087406787` |
| `full-prepared/overlay/db.go` | `fd91b173c4367ae69a366434fe39f64ae2364433903e7a313ba035a1ac829b3d` |

The full patch contains the original baseline compatibility changes plus the explicit manifest guard and bounded preparation/check scripts. It is a local proposal, not committed into H. `full-source-hashes.json` and the proof contain the complete source and construction provenance. Review changes against canonical H7e1 and inspect the strict guard before approving integration.

## Bounded validation and exact commands

One new Mac construction succeeded with explicit Go 1.26.0 darwin-arm64. Compiler SHA256: `ae2238c208f2f1f5a8faa3287acf5ced5d6b3c396dbf75e8f446e47905b0a75d`. This command performs `go list`, `go test -c`, and post-build `go list`; it does not execute tests or benchmarks:

```sh
env GOROOT=/Users/michaelseiler/.gvm/pkgsets/go1.25.5/global/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.0.darwin-arm64 GOTOOLCHAIN=local GOWORK=off GOMAXPROCS=2 GOMEMLIMIT=1GiB python3 /tmp/gomap-4895-baseline-compat-proposal/prepare_baseline.py --source-root /tmp/gomap-4895-baseline-compat --manifest /tmp/gomap-4895-baseline-compat-proposal/baseline-full-source-manifest.json --manifest-sha256 1a4475df41464a9619e4ad9ca494d0e8d96e183f26980fbea3f4e586dc0b9765 --output /tmp/gomap-4895-baseline-compat-proposal/full-prepared --go /Users/michaelseiler/.gvm/pkgsets/go1.25.5/global/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.0.darwin-arm64/bin/go --leaf-mib 0 --value-bytes 256 --threshold 1 > /tmp/gomap-4895-baseline-compat-proposal/full-prepare.stdout 2> /tmp/gomap-4895-baseline-compat-proposal/full-prepare.stderr
```

The output directory already exists; reproducing construction needs a fresh outside-source output directory. The executed self-check command is repeatable against this prepared artifact:

```sh
python3 /tmp/gomap-4895-baseline-compat-proposal/check_full_freeze.py --source-root /tmp/gomap-4895-baseline-compat --manifest /tmp/gomap-4895-baseline-compat-proposal/baseline-full-source-manifest.json --manifest-sha256 1a4475df41464a9619e4ad9ca494d0e8d96e183f26980fbea3f4e586dc0b9765 --prepared /tmp/gomap-4895-baseline-compat-proposal/full-prepared --freeze-sha256 35194ed0ad688d104b1760769f64d3c0d5788218c34bf5f0628ffb4fa6183e3c > /tmp/gomap-4895-baseline-compat-proposal/full-check.stdout 2> /tmp/gomap-4895-baseline-compat-proposal/full-check.stderr
```

It passed clean-source loading and rejection cases for dirty tracked source, unallowed additions, ignored source additions, wrong product identity, wrong external manifest digest, and changed external replacement input. With `--prepared`, it also exercised actual temporary tracked-source mutation, an actual unallowed Go addition, and actual compiled-overlay mutation, restoring the tracked/overlay bytes in `finally` blocks and moving its owned addition out of the product tree. The final frozen identity reloaded successfully. Test fixtures are retained in `full-freeze-selfcheck-fixture`; no tree cleanup was performed. A separate bounded quicksilver-loader check accepted the valid freeze and rejected wrong externally provided runtime HEAD, harness SHA, and freeze SHA. No benchmark ran in any check.

`full-check.stdout` states `baseline full-freeze guard checks PASS; no benchmark executed` and has SHA256 `2a576c3bf48ab657692c1a1d15e8fd7f9baebc35a79b307ac599a07c7980e248`. `full-prepare.stdout` has SHA256 `ca1fafed42ee2b84e250df165ef0569ec1844d6e80ebf4f35e542fbc56da2f8c`. Both stderr files are empty. Final baseline tracked diff is empty; the exact five allowed untracked additions remain.

## Historical pilot and remaining gates

The earlier compatibility review, `baseline-compatibility.patch`, `source-manifest.json`, `bootstrap.py`, and original pilot/prepared directories remain historical. In particular, the original pilot freeze `b755310e7ecf1e31bb12a880642b7533e65c81698b6a5bdb81faca0bf853b876` was not relabeled or altered. The new binary matches its binary hash because the Go replacements and compiled workload are unchanged; the new full freeze independently records `pilot=false` and `TREEDB_MEMORY_PILOT=0`. Binary equality does not turn that old pilot into retained evidence.

Remaining gates belong to root:

1. Independent Sol61 source review and explicit integration decision for this baseline-only proposal.
2. Reviewed/landed T/A and H integration, with actual final H canonical identity bound through hashes. H7e1 calls the newer `check_observations(..., filter_bytes, extra_stats)` adapter; shared T/A1d removes those arguments. Resolve that source seam in final H/T integration before construction. T memory Go and overlay hashes are unchanged between H7e1 and T/A1d; the Python capture helper is changed. This proposal faithfully uses H7e1 and does not claim the stages are interchangeable.
3. Separate fresh manifests/preparations for original baseline, maintenance control, and final product on the actual Linux toolchain and host. Current absolute-path/compiler inputs are Mac-specific and cannot be relabeled Linux. Recheck canonical source/replacement hashes after integration; the guard deliberately hard-pins H7e1 and must be narrowly updated through review when actual landed H changes.
4. Only after those gates, execute the authorized retained campaign with three/five fresh-process repeats, noise/spread checks, all required correctness/owner/placement boundaries, and native successful offline compaction, GC, and old-generation pin evidence. This lane did none of that.

No product-equivalence, cold-device, whole-RAM, performance, or retention acceptance claim is supported by this preparation. The canonical workload performs its reads before update/checkpoint intervals; this bridge does not create or claim concurrent reads during those intervals.
