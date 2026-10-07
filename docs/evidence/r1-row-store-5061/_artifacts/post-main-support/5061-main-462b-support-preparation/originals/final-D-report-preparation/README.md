# Private final D report formatter

`format.py` renders descriptive tables from an original lifecycle v3 packet and its neighboring `run-NNN.log` files. It reuses the reviewed frozen validator's `decode` and `raw_results` functions, checks original log hashes, and keeps every emitted calibration/final result plus the original packet in a new linked JSON projection. It never runs the validator's qualification entry point, opens a benchmark binary, executes Go, reads a server, or accepts evidence.

The selected parser was read from Git object `e4e7a3032405bef141010b1c80eb0f0db1cd6f74:scripts/r1_lifecycle_validate.py`. Its SHA256 is `dcfd45e233bc46a1c1f73ffaf2af840e5d8ab8ba312342ceb61b014e7c199968`. The formatter pins that hash and requires the packet to bind the same parser. `frozen-r1_lifecycle_validate.py` is an unchanged private copy for offline presentation. A changed schema/parser requires reassessment, not a hash override.

After the coordinator independently qualifies the actual capture, invoke once with a fresh derived output path:

```sh
python3 format.py /path/to/original/packet.json \
  --validator frozen-r1_lifecycle_validate.py \
  --packet-link '<published original packet link>' \
  --raw-root-link '<published raw packet directory link>' \
  --validation-log-link '<published independently pinned validation log link>' \
  --details-link '<published lifecycle-details.json link>' \
  --details-out /path/to/new/lifecycle-details.json \
  > /path/to/new/lifecycle-report.md
```

The caller chooses publication links. The validation-log link is displayed without interpreting its verdict. Qualification remains the existing frozen validator with the coordinator's independently frozen source/runtime/harness/landing/binary/packet pins; retain that command and log separately. Publish the original packet and raw logs, the derived report/projection, and the independently recorded validation log together. Existing binary-overlay publication supplies the neighboring executable required for full validator replay; this formatter does not replace that replay.

The report shows each process's final mixed-call totals and whole-call p95/p99, every phase's component census, medians of within-process adjacent and ingest-relative byte differences, and complete individual process trajectories with sampled heap cuts. Each process includes operation counts, maintenance durations, typed reclamation/protection/pins/debt, vlog GC classifications, final leaf/immutable-revision counters, before/after fallback slots/root IDs/LSNs, and exhaustive maintenance ownership/options/remaining debt. Nested original attribution and every file/classification remain in the linked JSON. Counter and timer classes overlap; the formatter neither adds retention classifications nor normalizes aggregate maintenance work into per-call costs. Zero is an actual recorded value; an unavailable predecessor is —.

This is finite direct-backend command-WAL-durable evidence, with the repeated bounded hot set and background pruning disabled. It has no A cached-wrapper/COW equivalence claim, SQLite comparison, ratios, new acceptance threshold, unlimited capacity claim, RSS, or unsampled peak. Reported maintenance work limits are not physical storage bounds. A held-view zero-handle release assertion and deleted-asset/reopen oracles come from the benchmark source; this renderer does not add emitted samples for them.

`diagnostic-format-smoke.md` and `diagnostic-details.json` use only the historical **rehearsal** at source `f79616f2aeb99b43af81877f26d646cca51177b9`: one process, eight epochs, 32 live rows, eight calls/epoch. They verify presentation/arithmetic, supply no current measurement or acceptance, and explicitly have no supplied independent validation log. The originals remain unchanged. `validation.txt` records the bounded local checks; `handoff.json` records file hashes and scope.
