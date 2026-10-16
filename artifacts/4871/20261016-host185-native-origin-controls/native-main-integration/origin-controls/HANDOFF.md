# Focused origin controls: stopped at second call

The selected sequence consumed2 of19 calls on frozen source e434542a59d55cf67c90d6a16c5787abeb908efda4c1c705033658b9ba8e0577,10,375 files. Full source archive9d60ef1a4d1960aa63a0fabbb99b55b57c61262ca9bdaa867160303994cbf7ed and34 selected test-file bindings were verified before execution. Commands, exact environment, tool/compiler identities, raw output, before/after hashes and actual PID/PGID/subreaper observations are retained here.

| Call | Result | Top RUN/PASS | All RUN/PASS | Elapsed |
|---|---|---|---|---|
| normal-TreeDB-db | PASS / exit0 | 31/31 | 75/75 | 12.34s |
| normal-TreeDB-internal-rootpublication | STOP / exit1 | 17/16 | 32/29 | 1.21s |

The second call failed `TestPrimaryFinishResourceRefusalAndFailureCustodyV6/retry` and `/failure` at durable_root_transaction_work_callbacks_v6_test.go:315:

```
resource allowance refusal lost original custody: ready=false err=<nil> calls=0 effects=0
```

Its parent also reports FAIL. All selected top-level names ran; the failed parent/children do not satisfy required PASS. The other16 top-level tests in that call passed. No diagnosis, edit, repair or retry was attempted. All remaining17 calls, all race controls and the one M4 cell remain unexecuted. This is partial failed evidence, not acceptance.

Both calls preserved the full frozen source before/after, joined their actual leaders and left no process-group or subreaper child, with no timeout, signals, residual cleanup or SKIP. Source/runtime leases are released. No actual33, real native publication/final driver, benchmark or allowance was executed/selected. Q32 RED/ACK0 and all original caps remain.
