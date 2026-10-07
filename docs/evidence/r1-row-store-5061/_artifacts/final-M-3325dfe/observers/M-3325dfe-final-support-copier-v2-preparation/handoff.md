# Private final-M support byte copier v2 preparation

Prepared for root review, transfer and later root execution only. No actual plan, remote source, publication stage, namespace or assembly receipt was read or created here. `copy-support.py` does not require or infer accepted A/C/D cost decisions. Any requested decision record is treated solely as a pinned text file. Qualification, acceptance, validation and publication remain root-owned.

The stage is fixed to `/home/mikers/gomap-r1-publication-stage-M-3325dfe-20261006`. Root must supply an independently frozen explicit JSON plan and its CLI SHA256, plus the existing core ledger path and independently frozen CLI SHA256. Each plan row has exactly four keys:

```json
[
  {
    "source": "/ABSOLUTE/ROOT/SELECTED/ORIGINAL-TEXT-FILE",
    "destination": "landing/example.json",
    "sha256": "ROOT-MUST-SUPPLY-ACTUAL-64-LOWERCASE-HEX-DIGEST",
    "bytes": 0
  }
]
```

This is a deliberately **nonqualifying shape example**: the hash is invalid and zero is not an observed size. No actual source/value is invented. `destination` is relative to the NEW `final-M-3325dfe` namespace, so the example lands at `stage/final-M-3325dfe/landing/example.json`. A namespace prefix in a destination is refused to prevent accidental double nesting. Root's plan can select original landing/source metadata, selected base13/final16 evidence, frozen configs, original receipts, actual results/decision records, CI logs, helper/review packages and example receipts. The helper makes no category or acceptance decision.

Root's later command uses actual independently checked hashes and a NEW private receipt outside the whole stage:

```sh
python3 -B /absolute/reviewed/copy-support.py \
  --plan /absolute/root-frozen-plan.json \
  --expected-plan-sha256 ACTUAL_ROOT_FROZEN_PLAN_SHA256 \
  --core-ledger /home/mikers/gomap-r1-publication-stage-M-3325dfe-20261006/SHA256SUMS \
  --expected-core-sha256 ACTUAL_ROOT_FROZEN_CORE_LEDGER_SHA256 \
  --receipt-new /absolute/private/NEW-original-assembly-receipt.json
```

The exact core ledger filename is supplied by root; the displayed `SHA256SUMS` is a command-shape example, not an inspected existing filename. Receipt parent directories must already exist, be directories and have no symlink ancestors.

All sources must be explicit absolute canonical paths to regular nonsymlink UTF8 files without NUL bytes. Parent paths must also be directories without symlinks. The copier reads no directories recursively, executes no command, and performs no network, SSH, Git, Go, workload, measurement, validator or report-formatting operation. It refuses duplicate destinations, path traversal, reserved ledger collisions, file/directory collisions, receipt aliases, and an existing namespace. Empty original text logs are allowed when their actual zero-byte size and hash are explicitly pinned. UTF8 Python/shell helper source can be included as text; every copied file and generated ledger/receipt has mode0644 and no execution bits.

Every source hash/size is checked before namespace creation, immediately before its copy and again after all copying. The plan/core ledger hashes are checked before and after, and the core ledger bytes must be identical. Copied destination bytes and mode0644 are also checked. The helper's own original and final source bytes are recorded and must match. `final-M-3325dfe/SUPPORT_SHA256SUMS` lists every copied physical file once, excluding itself, with paths **relative to the ledger parent / final namespace** (`landing/example.json`, without a `final-M-3325dfe/` prefix). The assembly receipt keeps `actual_copies.stage_relative` paths relative to the full stage for inventory. Root later creates the complete publication ledger and independently inventories every physical staged file, including existing core support and `validation/*` helpers outside this namespace.

Success writes a NEW private assembly receipt with actual argv, UTC, helper/plan/core hashes and sizes, each actual copy and source observations, and the support ledger hash. State is `PINNED_BYTES_COPIED_ONLY_NO_EVIDENCE_OR_NUMERIC_ACCEPTANCE`; there is no accepted flag. A failure preserves any partial namespace, copied files, ledger or receipt. There is no cleanup, retry, replacement or original rewrite. Root must investigate partial outputs before taking another action.

Preparation validation is static AST inspection only; the copier was not executed, including against synthetic plans. Input safety and byte checks are implemented for later root-owned review/execution; static parsing alone is not a tested runtime acceptance claim.

V2 corrects only ledger-path emission and its receipt scope field (`paths_relative_to_namespace: true`). The v1 preparation remains untouched at `../M-3325dfe-final-support-copier-preparation/`; `v1-v2.diff` binds the exact two helper lines. All copy guards, source/hash checks, outputs, acceptance limits and partial-failure preservation are unchanged. Static AST and diff checks only; no copier execution occurred.
