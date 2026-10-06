# Single-file test handoff

Parent HEAD: `61fbd36655a9a8641f1cacee013c8f2bf2dc1553`.
Changed allowlist: **only** `TreeDB/cow_close_notification_test.go`.
Final test SHA256: `d15253d599b953a09baa79ef182ddf5bfd8814960040beed8243e3e6e6ccd548`.

Local Darwin/arm64 Go1.26.0 normal, race, and treedb_safe targeted count3 all exit0: each18 test/subtest pass events plus1 package pass, zero fail/skip. Both existing notification tests and both command-WAL profiles ran. Raw output, exact commands/exits and toolchain environment are retained. Pre/post targeted source hashes match; gitdiff--check passes; public.go unchanged. Hosted Windows execution remains pending and is not claimed.

The actual old Windows RED remains bound to its original log/hash. Diagnosis is calibrated in diagnosis-and-change.md. The repaired callback test independently witnesses notification entry/completion and synchronous release, preserves real pressure/error/drain requirements, separates callback5s from finite storage30s watchdog, and joins Close before removing its owned temporary directory. No runtime or benchmark fixture changed. No commit/push/GitHub/remoteGo actions performed. Root owns integration and CI.

Packet files are literal, hash-bound by packet-manifest.json (which excludes itself).
