# One manual formatting successor to observer packet 5598

This is the single source-only successor requested by
`GOMAP-FOUR-GATE-OBSERVER-FORMAT-20261104-01`. The predecessor is retained at
`../four-gate-observers-source-20261104-01`, manifest SHA256
`5598e7d3aa39dec908740dad4d4b7264e449655b39fdae17210acf3c3835fe80`.
It was authenticated against all 25 manifest entries before and after this work.
The source owner remains thread `01a15a41-5e6e-7f21-9052-99e7406d0861`;
ROOT thread `01a18e7c-8c23-7fd2-8d55-b0486d938cd3` owns exact review,
hosted binding, publication and execution. No additional worker/controller.

Six Go files were edited by hand: the four added helper/test files and the two
instrumented fixture files. Changes expand compact control statements and align
declarations. The seventh candidate Go file, the caching seam, is byte-identical
to its predecessor; the separate baseline seam and both arms' original preimages
are also unchanged. No product worktree was edited.

`preformat/candidate/` contains exact predecessor Go bytes. `formatting.patch`
shows only the formatting delta. `formatting-inverse.json` retains the exact
before/after text for each changed hunk and was used to reconstruct every
predecessor file byte-for-byte. `source.patch` is the complete updated overlay
against original 4934, not a product branch commit. Existing overlay mappings,
test inventory, input bindings, Runner module paths/hashes and API decision
remain byte-identical to packet 5598.

`format-source-review.json` records the bounded text checks. All literal,
identifier, operator and comment text is preserved. A Python lexical comparison
models newline semicolon insertion and the optional semicolon before a closing
brace/parenthesis; its canonical token sequences match. This supports source
preservation and does not replace Go parsing, compilation, race checks or source
review. The original PRIMARY loop/tests and M0 default writer are independently
checked byte-identical. Six tops, 26 named outcomes and 24 leaves per mode remain
the expected inventory, not observed test results.

**Formatting is manual and provisional.** No local Go, gofmt, replacement
formatter, compiler, build, test, benchmark, module import, SSH, transfer, host
probe, push or dispatch occurred. Hosted Go 1.26.10 must check the exact sealed
Go bytes. Normal/race, format and compilation remain UNRUN. The stopped regex
text-analysis attempt is preserved in `analysis-attempts.json`; it is neither a
Go failure nor a qualification result.

## Controls resource scope and retained capture limits

The applicability review is retained by path/hash in `format-inputs.json`:
`primary-m0-hosted-binding-review-01/review.md`. Controls-only qualification
selects exact candidate 4934 and the six controls, using the maintained hosted
normal/race CI, setup-go cache and compatible guard resource/cleanup contract.
The unrelated Windows diagnostic's 1 GiB whole-construction cap must not be
imported into these controls. No baseline tree or unnecessary retained combined
control ELFs are required for that candidate-only checkpoint.

The original PRIMARY diagnostic capture still requires both exact source arms,
their own preimages/overlays, the original 1 GiB whole-root materialization limit
and all original capture/runtime/admission limits. No control result changes
that capture contract. The old Attempt module is retained source lineage and an
interface reference, not a ready controls resource policy or execution grant.
ROOT separately constructs/reviews the smallest compatible hosted binding.

All product acceptance failures, historical samples and unknown causes remain
unchanged. Host185 logical HOLD persists. There is no local runtime, new
performance evidence, product repair, merge readiness or acceptance claim here.
