# Close notification read-admission handoff

Source b9e5587fb01cb49e6eddd576b068e30c3ca2ec63, 6460 committed files, Linux185 Go1.26.3. Pre/post execution hashes match with zero drift. Immutable runner: /mnt/fast4tb/gomap-cow-execution-o2nauuzm/OWNED/c2-notify-repair. Source identity, environment, explicit run-gates.sh, original RED inputs/logs/provenance receipts, current raw JSON, gate-exits.tsv and process snapshot are retained here.

The new read-owner selector and Stats use TryRLock to refuse admission while exclusive or pending Close owns lifecycleMu. Close/reportError ordering is unchanged. This prevents both synchronous final-checkpoint NotifyError reentry and asynchronous worker notification reentry while Close waits for a worker. The asynchronous witness uses the actual existing public Close hook and a controlled callback worker; it does not claim an injected background flush failure.

Original730808 source timed out both deterministic witnesses (two command-WAL profiles each), exit1; original sources and execution qualification are retained. At the repair, normal22.245s, race29.714s, safe22.290s count3 each pass147 test/subcase events plus1 package event; vet is empty with exit0. Actual read-vs-Close output validation, callback-to-Close, EOF chunked final frontier, public contract, empty-batch barriers and exact changed durability entrypoint inventory are included.

The broader unchanged seven-package normal/race/safe/vet730808 evidence remains in ../read-repair, with its own6459file identity and post-run drift0. This delta changes only public.go lock acquisition/comments, documentation and the new notification witness; no backend/cut/frame/flush/decoder allocation owner changed. The older packet is not relabeled as execution of the new candidate.

Writer correctness jobs have exited; runner released to coordinator for exclusive timed diagnostic collection. Whole C2 independent acceptance and final cost packet remain coordinator gates.
