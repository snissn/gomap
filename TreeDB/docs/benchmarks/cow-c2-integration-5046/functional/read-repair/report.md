# Public read-owner and lazy workspace validation

Exact source 730808f7048f90d76e36ca092162222cc45e4b03; 6459 files match before and after execution on Linux185 Go1.26.3. Full commands and matching environment are in run-gates.sh; all normal/race/safe/vet exits are zero. The changed durability entrypoint inventory is a separate exact-name supplement, with its script and exit receipt retained. Counts in summary.json exclude package events.

The actual public Close race asserts every successful value, revision, presence and successor against the seeded pointer tuple; closed admissions return ErrClosed. View callback can call Close. A genuinely exhausted shared budget permits cached metadata/inline/tombstone reads while pointer/backend decoding and internal View copies refuse and resume after drain. These gates also retain the actual chunked-overwrite Close EOF regression and broadened baseline snapshot, tree and resource coverage.

The subsequent b9e5587 TryRLock repair changes only public read admission/Stats lock acquisition, its documentation and a new notification test. Its affected execution is retained separately in notify-repair. This seven-package packet remains applicable to unchanged source; it is not relabeled as execution of b9e5587.
