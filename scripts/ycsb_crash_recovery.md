# Public process-crash correctness control

This separately labeled control is independent of timed YCSB and the checkpoint/
clean-close deterministic fixture. `ycsb_crash_recovery.py run` starts a fresh,
separate DB and two owned server processes. Current implementation and offline
controls do not establish any product recovery result or complete issue #5335.
Do not launch a product control without the coordinator's execution gate.

The native `crash-write` path uses the existing public native driver's ordinary
`AckVisible`; Mongo uses the official driver with `w:1` and no `j` request. A
single client issues 64 sequential whole-document inserts in the separately named
`ycsb_crash_recovery` collection/database, with ten typed 100-byte fields per row.
There are no updates or concurrent ambiguous ACK order. Native JSON fields are
canonical base64 strings; BSON fields are subtype-0 binary. Mongo JSON with byte
fields remains unsupported. Native BSON still requires the feature dependency
that was absent from the previously retained unsupported control.

Before requests, the helper creates and fsyncs `expected.json`. After **each**
original insert returns, it appends the actual checked Result and ACK outcome to
`crash-write.json.acknowledged.jsonl` and fsyncs it before issuing another request.
A failed/ambiguous return is retained and stops the helper. It is never retried,
never added to required recovered successes, and prevents this bounded attempt
from reaching the crash step. Full admission requires all 64 checked successes.
The helper checks the exact precrash scan and closes only public client network
resources. Its native `Client.Close` closes the connection; Mongo `Disconnect`
closes driver resources. Neither driver requests FlushAll/checkpoint. Service
connection cleanup releases cursors/authentication/pinned connection resources,
not the service owner. No server graceful Close precedes the crash.

The expected manifest, ACK journal, typed snapshot, original client receipt and
launcher receipt live outside the service DB. The runner checks hashes, fsyncs
these files, and fsyncs the target, output and output-parent directories to retain
file names and fresh directory publication. It then fsyncs the barrier receipt.
It never calls global sync or fsyncs the service DB to manufacture durability.
Original owner observations must bind source/ELF/profile/COW-or-legacy/data
identity and actual PID/start ticks. Raw procfs TCP/listener inode and owned fd
observations bind the loopback endpoint to that same process before clients.

The runner pins its direct child before any wait/reap, rechecks its actual
identity and sends SIGKILL through the pidfd, then waits boundedly for exit -9.
It reopens the same source-bound executable and DB inode with an independent
process/owner and verifies through the same public driver. Only the reopened
owner is gracefully closed. Spawn/pin/readiness/helper/timeout failures retain
failure receipts and use bounded owned-child cleanup and reap. All actual child
environments remove GOMAXPROCS. The private owner checkpoint environment is
removed from both service launches.

The validator uses exact JSON types, nanosecond chronology, original source
contracts, actual operation Results and canonical typed snapshots. Durable
ordinary ACK requires all 64 exact rows after process crash. Relaxed command WAL
allows an exact acknowledged-order prefix (including empty); no-WAL allows any
subset of complete exact rows, without inventing a global ACK-order prefix. No
profile permits invented keys, torn fields or changed physical value types.
These contracts come from each bound source's `docs/contracts/DURABILITY.md` and
`TreeDB/mongo_gateway/COMPATIBILITY.md` (ordinary ACK differs from synced ACK).
Baseline and candidate use separately supplied source/ELF/owner build manifests;
normal defaults are preserved.

**Scope limitation:** the fixture requests no explicit checkpoint. Public
service defaults can perform automatic checkpoint/root work. No race-free
background-checkpoint exclusion witness or actual recovery replay witness is
currently available here. Output therefore always records
`unproven-background-checkpoint-exclusion-and-wal-replay`. A PASS establishes only
this bounded public ordinary-ACK **process**-crash state check. It is not proof of
uncheckpointed WAL replay, power loss, all batches/domains, arbitrary in-flight
writes, or full recovery certification. No-WAL has no command WAL to replay.

The CLI requires fresh `--output`, explicit `--target`, `--profile`, `--format`,
loopback `--address`, absolute `--server-binary`/`--fixture-binary`, a versioned
`--server-manifest` in the maintained harness `servers` schema, and
`--fixture-manifest` with exact schema 1, `exe_sha256` and `source_sha256`, plus
`--fixture-source`. `--go` selects the buildinfo tool. Deadline flags bound helper,
cleanup, readiness and complete attempt. `validate DIRECTORY` reads retained
artifacts without starting services. Existing receipts remain historical and
are never rewritten to acquire newly required evidence.

Tests in `test_ycsb_crash_recovery.py` use explicitly synthetic packets and tiny
stub processes. The compiled `testdata/ycsb_crash/synthetic_stub.go` implements
**no database, public storage operation or WAL**. Its real CLI test checks packet
production, identity, custody, kill/reap and final validation only. Target/profile/
version permutations and negative controls are offline consumer tests.

The separate deterministic native `write` fixture now retains the timed Result
around its single original public `Checkpoint(ctx)` call, including the actual
checked return/error. Its attempted/succeeded flags derive from that Result.
Old flags-only receipts remain historical; they do not satisfy the new required
Result shape. This change adds no extra checkpoint call and does not infer a
server-internal duration from client elapsed time.
