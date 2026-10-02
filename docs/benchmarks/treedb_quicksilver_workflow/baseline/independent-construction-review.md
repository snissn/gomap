# Independent F4895 baseline full-freeze source review

**ACCEPT for the original-baseline construction only.** No blocking finding in the reviewed candidate or bounded loader checks. This decision does not approve integration, execution, retained measurements, product equivalence, or performance. It does not qualify the unprepared maintenance control.

## Exact reviewed identities

- Proposal directory: `/tmp/gomap-4895-baseline-compat-proposal`.
- Product: `/tmp/gomap-4895-baseline-compat`, actual HEAD `ef22ce55b85524de2707453f22c5f8cd6ba89eaa`, tracked tree `c023d39fee5b91e3764b7669c415b4182df429b3`, TreeDB tree `d3dcef28f252494cf083b8058ee9e06779949842`.
- Canonical H: `/tmp/gomap-issue-4894-harness`, actual HEAD `7e1c51d41bcc0392fcb17dec12e46fdab42449bc`, clean status.
- Shared T/A: `/tmp/gomap-quicksilver-measurement-harnesses`, actual HEAD `1d4826dda8ae619cf95d810c53d6bd5fe10a8c3a`, clean status. An explicit `git merge-base --is-ancestor H T/A` returned 1; T/A does not contain H.
- Product tracked diff is empty, exactly the five manifest additions are untracked, and `git ls-files -v` contains no non-H flags hiding tracked changes.
- Root and nested `TreeDB/AGENTS.md` were read in both actual product and canonical H checkouts. Their persistent-vlog, durability/GC, pre-alpha, and standalone capture contracts apply. This proposal changes no committed storage format or production source. No further nested instructions were found under the touched TreeDB/scripts paths.

The handoff was treated as an assertion. Actual artifact hashes, patch application, identities, and loader behavior were independently checked.

| Artifact | Independently verified SHA256 |
| --- | --- |
| `baseline-full-freeze.patch` | `87b6f7dd33a3eae4d52bb06716c1b4263174e6b5dff16b6bbe82ce502f43c728` |
| `baseline-full-source-manifest.json` | `1a4475df41464a9619e4ad9ca494d0e8d96e183f26980fbea3f4e586dc0b9765` |
| `full-prepared/freeze.json` | `35194ed0ad688d104b1760769f64d3c0d5788218c34bf5f0628ffb4fa6183e3c` |
| `full-prepared/memory-budget.test` | `56275e8b45b867445b03c9434889d6a1dad862cd1d2c25209f2f751ed5c0a633` |
| `full-prepared/overlay/overlay.json` | `6bc66019b45ebfef148a055a3ad2fa6a3cd9804e5a8563d1831d9ffad6813ecd` |
| `full-prepared/overlay/identity.json` | `4800b8bbe414c7d6e06814d50600731fc87311508cb0929dce8245e087406787` |
| `full-prepared/overlay/db.go` | `fd91b173c4367ae69a366434fe39f64ae2364433903e7a313ba035a1ac829b3d` |
| `full-source-hashes.json` | `52b973bbd3b5153db67b3ee840aa6797d681b51356a406f4cf5896c7831cbf94` |
| `full-preparation-proof.json` | `680301e818e5d39edac59577b5d6b8ac17132352ff063957c772440e3845b096` |
| `TreeDB/memory_budget_bench_test.go` | `68599f74579bcb0f5dba0155a70120ec2a480185044f0edc09cca930191e4508` |
| `TreeDB/quicksilver_workflow_bench_test.go` | `84d0c67576d3e3396baa5c421bcf6ac6158958787e071d45e808337d148c3736` |
| `check_full_freeze.py` | `b1887f88cfdcc10fbb7107d63c0596b936d17d4f0de7bbbff163d283eea6a58e` |
| `prepare_baseline.py` | `573c0e069e8c86babc2052a3c7064358f0eb1d9f66fe662a1df7f7e5d4bb1655` |
| `scripts/treedb_memory_budget_capture.py` | `21a530d7b8addff48ff2280f9949249d61ce25f743378ab4f022afcb45da728a` |
| `scripts/treedb_memory_budget_overlay.py` | `156abcee681ea4de1366df75e6dfe868ead23a240334f33d331804f529730167` |
| `scripts/treedb_quicksilver_capture.py` | `4873183be0d55ae945987cbae42197a3f6730460061d5b7092d1a9ae1a0ea428` |

Canonical H test hashes were verified against the actual canonical worktree: memory `03b389e475374af7157111b4a8feea72a22124aa4fec5f3342d512ea6cbad462`, quicksilver `05d2a485efd14d43e3f60f715761e3cbfb50b7a31aa11e55f44e8d4ffc42da96`. Actual compiler executable hash is `ae2238c208f2f1f5a8faa3287acf5ced5d6b3c396dbf75e8f446e47905b0a75d`. All 21 proof-listed artifact hashes and seven source hashes match. An in-memory unified-patch application to actual H reconstructed all six changed/new files exactly; the unchanged overlay generator also matches its pinned source hash.

## Checked requirements and source evidence

1. **Original production identity and baseline exception:** `scripts/treedb_memory_budget_capture.py:149–179` requires the externally supplied manifest digest, supported original/control HEAD, canonical H identity, actual product HEAD and both trees, an empty tracked diff against HEAD (including staged changes), exactly five nonignored additions, no ignored source-suffix additions, regular nonsymlink addition files, exact addition hashes, and exact canonical Go hashes. The original product currently meets those conditions. The control is permitted by source but has no accepted construction here.
2. **Normal clean gate preserved:** `scripts/treedb_memory_budget_capture.py:186–196` enters the exception only with an explicit manifest, rejects pilot mode there, and leaves the existing ordinary full `git status --porcelain --untracked-files=all` clean-check intact. A bounded call on this real five-addition product without the exception rejects before output creation or Go invocation.
3. **Canonical compiled test replacements:** `prepare_baseline.py:15–44` binds the executing preparation path, reads the manifest-pinned external replacements, writes both overlay replacements, and records canonical/replacement hashes. The prepared `overlay.json` maps both actual original paths to the expected replacement files; both replacement hashes match the external reviewed Go files and appear in the frozen compiler-input map. `scripts/treedb_quicksilver_capture.py:102–117` follows the actual replacement mapping and requires the compiled path to be hashed before accepting full external runtime/H identity.
4. **Capture/tool inputs and run rechecks:** `scripts/treedb_memory_budget_capture.py:169–179` pins both replacement Go sources, memory/quicksilver/overlay Python sources, preparation helper, and actual Go executable through the external manifest. It binds the executing memory capture path; quicksilver `load` binds its own path. Preparation also checks the actual resolved compiler (`:203–206`), hashes go-list package/test/embed/module inputs plus Go tool/include files, repeats go-list after the build, compares all inputs and source identities, and reruns the manifest guard (`:207–228`). Current loader independently rehashed all 4,417 recorded compiler inputs, binary, overlay metadata/setup, source files, compiler, and external manifest inputs. Build/list exit codes and both stream hashes were checked. Memory and quicksilver run callers recheck current identity before/after execution and validators compare them to the frozen identity. No run caller was executed here.
5. **Full versus pilot:** The exact reviewed freeze has `pilot=false`, environment `TREEDB_MEMORY_PILOT=0`, full canonical key/update/read controls, and an external freeze digest. The baseline preparation refuses pilot. Both packet validators reject retained pilot evidence. The historical pilot is not evidence for the full preparation; equal binaries do not change that distinction.
6. **Equivalent workload construction:** The complete Go tests and Python callers were read and their exact diffs against H inspected. Apart from filter capability adaptation, packet schema/capability labeling, and the missing quantile helper copied verbatim from H's existing algorithm test, the fixture generation, deterministic query seeds/distributions/miss sequence, owned single/64-key reads, CRC32 consumption, full-byte hit/miss verification, present-empty sentinel, placement checks, synchronous update/checkpoint phases, reopen/final-close checks, command-WAL LSN condition, file inventory, timing/allocation and owner samples are unchanged. `TreeDB/quicksilver_workflow_bench_test.go:194–258` retains deterministic queries and CRC checks; `:77–140` retains placement, phases, LSN and lifecycle; memory `:96–151` and `:278–435` retain fixture/reopen verification.
7. **Absent negative-filter capability:** Go quicksilver rejects nonzero filter bytes before `Open` (`:42–45`), and configuration checks reject any stats key with `treedb.negative_lookup_filter.` prefix. Its distinct packet schema and `negative_filter_support=absent` do not invent engine-zero observations. Python quicksilver rejects filter-on and requires the distinct schema/capability; memory `check_observations` (`:261–278`) preserves all other required observations and rejects the prefix at initial, closure, reopen, every phase/before boundary, and quicksilver post-reopen-GC boundary. Bounded negative checks confirmed each boundary rejects even a zero-valued new filter stat.

## Commands and bounded checks

Read-only shell inspection used `cat`, `sed`, `nl`, `rg`, `git status --short`, `git rev-parse HEAD HEAD^{tree} HEAD:TreeDB`, `git ls-files -v`, and `git merge-base --is-ancestor` on the paths above. Two `python3 -B - <<'PY'` assertion blocks performed the 39 loader/guard checks and exact patch/hash verification; their outcomes are retained in `baseline-independent-scratch/checks.json` and `baseline-independent-scratch/patch-proof-check.json` next to this report. Guard/loader rejection cases used mocks rather than changing product or proposal files.

The retained runnable loader check is:

```sh
python3 -B /Users/michaelseiler/dev/snissn/gomap/tmp/treedb-quicksilver-graph-20261001/baseline-independent-scratch/loader_check.py
```

It loads the exact freeze, verifies full mode and canonical loader acceptance, rejects wrong external freeze/runtime/harness identity, and rejects a changed compiled quicksilver replacement through a mocked digest. Both existing capture and overlay `self_check()` functions also passed during the larger assertion block.

All 39 checks passed. No `check_full_freeze.py` mode was run because its fixture path writes into the proposal and its `--prepared` mode mutates product/compiled-overlay bytes. No Go executable was invoked by this reviewer. No build, benchmark, Linux operation, GitHub operation, commit/push, cleanup, or delegation occurred. Writes were confined to this requested report and owned adjacent review scratch. This validates source construction and frozen loader identities; it does not independently reproduce the historical compilation or execute Go fixture tests.

## Findings, caps, and stop rules

**Blocking findings: none for the exact original-baseline construction.** No corrective source patch is proposed.

**Required integration gate, not a current self-consistent candidate defect:** H quicksilver `scripts/treedb_quicksilver_capture.py:90` calls `check_observations(packet, leaf, filter_bytes, extra_stats)`; T/A stage memory helper `scripts/treedb_memory_budget_capture.py:213` accepts only `(packet, leaf)`. Directly mixing those stages raises a TypeError. Root must resolve the adapter seam in reviewed/landed H/T integration and refresh narrowly reviewed hard pins for the actual landed canonical source; this review does not accept H/T interchangeability.

Remaining gates: reviewed/landed T/A and H; fresh independent manifests and preparations for original baseline, maintenance control, and final product on the actual Linux host/toolchain; authorized retained fresh-process repeats and noise/spread acceptance; successful native offline compaction/GC and old-generation pin evidence; complete runtime correctness/owner/placement checks. The Mac absolute paths/toolchain and provisional H may not be relabeled as landed or Linux evidence. The canonical reads occur before update/checkpoint intervals, so no concurrent-read-during-update claim is supported.

Stop before integration or retained collection on a changed patch, manifest/freeze digest, canonical source identity, external helper/compiler/replacement, tracked product bytes, unallowed addition, unresolved H/T adapter, or mismatched full/pilot mode. Construction ACCEPT is restricted to the exact artifacts above and does not waive those gates.
