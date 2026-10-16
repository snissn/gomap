# Maintenance diagnostic bootstrap failure and source portability repair

PR #5137 landed at `9165c53e4d82d12231af8404b0b7c5aea13530a6` after 50 passing exact-head checks and independent source review. The originally failing nativewire restart test passed on one documented same-head CI retry. Its original failure, classification, single retry and final gate receipts are retained without relabelling the first attempt.

The one authorized fixed 5bca/a17fe fourteen-call diagnostic, run 38830037922 attempt1, failed in the pure Python contract check before preparation: **zero Go builds and zero benchmark calls**. The artifact upload succeeded. Its original ZIP is retained and independently verified against GitHub's exact size and SHA256 digest, alongside safe extraction, raw logs, run/job metadata and the original grant. This is a harness validation failure, not a performance result. No unchanged-run retry or second diagnostic dispatch has occurred.

The original test hashed ast.dump output generated with Python3.10. Python3.12 adds empty FunctionDef.type_params to that representation. Emulating that documented schema change reproduces the exact hosted mismatch. The three function sources are byte identical to the original reviewed collector commit22e605b5c738ce759c5a7b8a3195f8d3ed53132c. The source repair hashes those exact source segments instead, preserving fixed product inputs, schedule, parser, caps, quietness and ephemeral-runner workflow.

PR #5138 head `8db93fec7176d23bad95168954884da807ee1da4` changes one test file, 8 additions/5 deletions. Ten pure controls pass under actual Python3.9.23 and3.10.12. Independent exact gpt-6.1-sol review ACCEPT is retained. Python3.12 AST-shape reproduction is not an actual successor Python3.12 execution. Required hosted CI is still pending at publication; no merge or new diagnostic collection is claimed.

Product ordinary PR #5127 remains at2588 with its original M0 failure and unaccepted maintenance costs. The separately reviewed a17fe packing successor has no new performance sample from this failed bootstrap. This artifact does not claim native publication, Q32 progress, ACK or final graph qualification.

Every retained source/stored file is bound by manifest.json, including decompression. Small original attempt.zip bytes are included. No product source or existing gate is modified by evidence publication.
