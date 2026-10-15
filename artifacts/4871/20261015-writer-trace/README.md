# One production-path writer-tail diagnostic

Single retained AFTER binary invocation; no source change, rebuild or retry.
All public selector/output/ACK/layout/Close gates pass. The 7.535ms writer
maximum includes a 7.505ms unix.Dup interval in original command-WAL stable-token
capture (99.60%). There is one Dup per measured CommitAt; the other63 total
9.728us. No overlapping GC or separate Go runnable delay is recorded.

Kernel work versus waiting or descheduling inside Dup is unknown. This trace
does not prove GE causality, a safe reuse optimization, repeatability, speedup,
native pruning viability or final acceptance. The prior +15.89% multilevel
pointer total concern remains. Do not trim tails from acceptance results.

The exact retained binary is excluded from Git; its SHA and the durable source
artifact16d1b295 are recorded in manifest.json. Original trace, raw benchmark,
call chain, command/environment and analysis records are included. No further
benchmark collection or cleanup target is authorized by this artifact.
