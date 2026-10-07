COPIER PREFLIGHT FINDINGS; REPORT LAYOUT CLEAN.

Reviewed copier `dff812a25f9cbd82fde6044b724addc6b49f63a71e3e9949312a548ec7133b42` and report assembler `fe81e26e777e49c78fd2e7b81e2a556e0fd4b8be2facfb8be64f945278372a5f`. Companion JSON records exact original/new pins and diff hash.

Two bounded preflight gaps at copier `destination()` line71:

- Consumer grammar mismatch: `reports/final note.md` passes copier guards and is emitted into SUPPORT_SHA256SUMS, but public executor `rel()` permits only `[A-Za-z0-9_.-]+` per component and refuses that ledger. Match its grammar or independently preflight the entire actual frozen plan with it before use.
- Reserved generated-ledger subtree: `SUPPORT_SHA256SUMS/child` passes preflight and creates a directory at the future ledger path. Ledger write then fails after copies. Reject any destination with LEDGER as its first component.

The corrected v2 ledger base is right: entries are namespace-relative and therefore resolve under the ledger's own parent. Full-stage paths remain in the separate receipt inventory. Explicit hash/size, UTF8/noNUL nonsymlink regular inputs, original source/plan/core byte checks before/after, exclusive mode0644 outputs and outside-stage receipt are otherwise coherent. Failures preserve partial outputs and no acceptance is inferred. These examples were determined from source only, not executed.

Assembler diff is limited to a fixed sibling public-replay proof prefix, an optional receipt selector and that selector on the single public replay receipt. Other receipt paths, substitutions, verbatim history/fragments and actual-value guards remain unchanged. Report and manifest templates are byte-identical to the frozen originals. Keeping actual replay proof outside `_artifacts` preserves its whole immutable ledger snapshot.

No actual plan or stage was inspected. No helper execution, synthetic-success test, SSH/network/Go/capture/GitHub/source/E write occurred. Root should repair those two guards or independently enforce both restrictions across the exact actual plan before activation. Presentation remains nonqualifying. Released.
