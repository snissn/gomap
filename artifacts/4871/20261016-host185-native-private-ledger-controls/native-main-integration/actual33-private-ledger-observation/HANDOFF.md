# Selected actual33 command: runner setup failure

The one authorized Go command launched and exited 1 before compilation or test RUN. Raw error: `go: creating work dir: stat /mnt/fast4tb/gomap-mvcc-4871-20261015/native-main-integration/actual33-private-ledger-observation/tmp: no such file or directory`.

This is an operator/runner setup failure: the adapted runner created a per-command temporary directory, while the exact grant environment names the top-level `tmp` directory, which was not created. The runner and raw receipts are preserved unchanged. No retry, source repair, additional Go command, test body, observer payload, or native publication occurred. The attempt is consumed pending root disposition; no product PASS/FAIL or selected-ledger numeric result is claimed.

Actual PID/PGID 1110239/1110239 exited and joined; runner/subreaper PID 1110200. Group and direct/adopted children empty; no timeout, signal or intervention. Session runner joined exit0 after recording command failure.

All 10,389 source archive members and live files were verified; before/after manifests and index hashes remain equal. Manifest `8f3a55720f95ec0a9cdf7e18a0360fd09b9385d41a25b28076a3d95157db9889`; separate full archive `c5699a0ffd003eac5553fc732e37ac9b8732bd90e495e71473c6a13f7c8518b6`; root-recorded staged tree `d63bc97e893e518c87687831a6e0a01a357268f2`.

Pinned Go1.26.3/compiler identities, exact argv/environment, original grant template executed=false and actual command executed=true, full raw and process receipts are retained. The raw summary's actual33_executed=true means command launched; zero test bodies actually executed, explicitly reconciled in handoff.json.

Source and runtime leases released. No source/Git/index/GH changes, benchmark, allowance or native finaldriver/publication/Finish. Original Q32/1MiB/33/80/4096 remain RED/ACK0. No selected-empty completeness result was reached; every required observation remains unavailable for this attempt.
