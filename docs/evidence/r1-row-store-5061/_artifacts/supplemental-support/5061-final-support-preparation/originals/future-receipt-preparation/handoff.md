Root-controlled retained D receipt preparation

This private executable template has not been run. Candidate inspected locally is f79616f2aeb99b43af81877f26d646cca51177b9; its landing is NOT established. No repository writes, Go invocations, captures or external writes occurred in this task. Python AST syntax check passed.

Root must first independently verify reviewed actual tooling landing, merge ancestry, measured clean source, expected runtime/harness inventories, matching host/toolchain and ownership of the capture worktree. Fill config.template.json from those observations, including landing_observation (actual merge/review evidence and observed UTC). Empty trusted fields deliberately prevent execution. Never extract those values from a submitted packet. Choose a NEW capture directory and a NEW private receipt directory; the latter remains outside capture/source. Preserve the filled config as a root-controlled input. Review this template before use.

Authorized invocation only, using the config's exact canonical lock:

```sh
flock /home/mikers/gomap-r1-evidence-20261005/timed-capture.lock \
  env R1_RECEIPT_LOCKED=yes python3 /root/private/observe.py /root/private/filled-config.json
```

On185 the equivalent configured lock is /home/mikers/gomap-r1-correctness-185.lock; use the lock root coordinates for the assigned host/window, never concurrent own timing. The locked flag is a caller contract, not proof of OS lock ownership. Root must use the stated flock command.

The parent observer verifies exact clean source/lineage and independently computes inventory using the reviewed source helper, freezes inputs/environment and its own script hash, then invokes audited capture. A private R1_GO wrapper transparently delegates list/env/version commands and admits only the exact collections test compilation. It observes source before/after compiler execution and freezes compiler argv, actual exit, Go executable hash and produced binary SHA before returning success, so the audited driver cannot start its five processes first. Failed builds preserve observations and never produce acceptance.

After the audited driver finishes, the parent records its exit and log digest; only exit0 plus unchanged source/build identity/executable permits freezing the exact original packet byte digest. Binary expectation comes from the earlier build receipt; packet expectation comes from the parent's observed completed file, never packet metadata. It invokes the frozen validator with all six external source/landing/runtime/harness/binary/packet bindings from root receipt. Require validation-exit.json exit0 and actual root acceptance; trusted-completed-receipt.json certifies the observed successful completed driver run, not standalone acceptance. Capture's internal self-check remains separate.

Trust limits: this is root-coordinator observation, not an independent hostile-host attestation service. Root controls the observer, checked-out reviewed helpers, compiler/cache/modules/OS and unchanged-source ownership. Inventory computations reuse audited code; the wrapper records actual compiler invocation and filesystem-produced executable, but does not independently derive compiler semantics or inspect kernel exec events. The parent observes the audited driver's completion; raw child-process truth is trusted to that reviewed driver, which launches exactly the observed executable and checks unchanged bytes. No adversarial concurrent source/binary writer may own those paths. Snapshot/identity checks are successive observations, not an atomic security boundary. External landing/review truth is root-provided and cannot be established by ancestry alone. The minimal build environment clears unknown caller flags/GOENV; child environment remains the audited capture's fixed contract. Performance qualification still needs actual load/spread and all physical guards; a receipt does not satisfy them.

Offline relocation: preserve original packet/raw logs/binary bytes and separately trusted receipt, run frozen validator on relocated packet with receipt's six expected values. Historical compiler/invocation paths remain provenance; no rebuilding, rehash-and-rebind, original worktree, GitHub/network or original wrapper-path execution is needed. Failed prior packets remain untouched.

Root next decision: review template, select exact landed f796 successor/source and inventories, authorize the assigned fresh retained5x5 capture, then perform independent receipt-driven validation. No run is scheduled by this handoff.
