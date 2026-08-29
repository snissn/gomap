# TreeDB 10M text/hybrid failed artifact (#4329)

This directory retains the first exact 10M candidate execution from PR #4435. It is preserved failure evidence, not a validator-qualified artifact and not passing 10M evidence.

## Candidate

- source commit: `c1c644ead34839570bcdcdac6ee4747cbbbaca6e`
- source tree: `fb8f32dac0889e22fee0bb9070a9f9c56d8939d1`
- TreeDB subtree: `848d455b64cdd13ba7d76e66a2934f32734eff40`
- harness subtree: `15a76c34bc05ca39d55f9bb003faa6054974bb6b`
- binary SHA-256: `b02847b9459f9d0cfbe26c2f7f34bc631f2944d291d6f27fd38ef7f20d5ea23a`
- frozen config SHA-256: `80341e126af74686137d8bc6b6ea2ff3ea1163803791cf7b1aa4872dcc88ab61`
- original local artifact root: `/Users/michaelseiler/orca/workspaces/gomap/4329-final-10m-v1`

## Result

The exact 10M load completed in 8,566.689 seconds. The query matrix then failed on the warm-up for `hybrid_text_scalar_rare_no_docs`: the fixed 655,360 text-candidate bound was insufficient, so TreeDB correctly failed closed with `exact_bound_insufficient`. Only the `load` phase completed. The required reopen, concurrency, maintenance, backfill, text-only, and source/chunk phases did not run.

The candidate-authored report predates the failure-cleanup persistence fix and therefore has a blank cleanup row. The process defer removed the fixture paths. `post_run_cleanup_observation.json` records a clearly labeled external observation that all five paths were absent after process exit; it is not part of the candidate-authored report or validator seal.

## Integrity

Run from this directory:

```sh
shasum -a 256 -c SHA256SUMS
```

`scale_report.json`, `scale_report.md`, `run.log`, `run_status.json`, `resources.txt`, the exact command/config/context, and binary provenance are retained verbatim. No seal or validation log exists because the runner only seals a successful complete artifact.
