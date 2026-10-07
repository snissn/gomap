# Derive-only config template (not executed)

`derive-configs.py` is a future root-controlled action. This preparation parsed its AST only; it has not run, invoked Go or touched a remote host. Root must review it, fill the blank `root-inputs.template.json` from actual external landing/review authority, copy both privately to111, and invoke after actual merge. The script refuses blank/unauthorized inputs and requires the exact clean selected Linux checkout under the canonical lock. It derives actual A/C and D manifests from reviewed helpers in that checkout under pinned Go1.26.4 environment. D executes actual `go list` through `source(real_go)`. It exclusively writes a NEW directory containing A/C/D configs, separately frozen source manifests and derivation receipt; it does not run an observer or capture.

Future derivation command, once root has independently verified landing and prepared completed inputs:

```sh
flock /home/mikers/gomap-r1-evidence-20261005/timed-capture.lock \
  env PYTHONDONTWRITEBYTECODE=1 R1_RECEIPT_LOCKED=yes \
  python3 /home/mikers/gomap-r1-final-observers-20261006/derive-configs.py \
  /home/mikers/gomap-r1-final-config-inputs-20261006/root-inputs.json \
  --config-dir /home/mikers/gomap-r1-final-configs-20261006/M
```

Each observer then runs **independently**, using `M/C-config.json`, `M/A-config.json` or `M/D-config.json` in the same single-lane flock commands in handoff.md. Do not batch captures or infer approval from successful derivation. Inspect/freeze the derivation receipt before observer execution. All configured capture/receipt leaves and config directory must be distinct/nonexistent, outside checkout and each other; existing capture parents are needed for A's actual df/device observations.

Source derivation is not a build receipt or capture acceptance. Root's landing-verification boolean and observation text encode already verified external authority; this script does not establish GitHub truth. The lock marker does not establish actual flock ownership. Preserve every original and any future failed derivation output. The blank template remains preparation, not final configuration.
