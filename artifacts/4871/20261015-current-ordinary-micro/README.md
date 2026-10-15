# Current ordinary candidate vs pre-sprint and recent main

Current clean source a934ea4c7a90b95bc15d9feb6ee9a3edde6502eb rebuilt actual db,
caching and public TreeDB test binaries.2452 repository source/test/embed inputs
have identical before/after censuses and match exact source commit blobs.
Go1.26.3; GOWORKoff; local toolchain; CGO1; CPU0/GOMAXPROCS1 per run.
Baseline28aea01 and main3f87d74 binaries are unchanged hashes retained in artifact
854ce8c35d912783dea92faf26b058651071b85a; their original source/build bindings
remain authoritative there. This packet does not relabel native working source
as the compiled ordinary candidate. No source, Git index or HEAD was changed.

Two balanced short rounds, five original benchmark rows, all30 invocations PASS,
19.69s measured process wall time. First source enumeration rejected a generated
test-main cache path before builds. The first driver then stopped parsing the
successful durable row's extra custom metrics. Parsing was corrected and resumed
from verified existing logs/binaries without repeating any measured invocation.
All33 successful build/run receipts and raw logs are preserved. This remains a
short diagnostic, not formal retained qualification or a statistical claim.

Medians: pre-sprint / recent main / current ordinary:
Get467.25 /338.65 /462.05ns;512/0/0B,1/0/0alloc.
Batch61397.5 /59052 /61146ns;152717/152783/154891B,281/281/285.5alloc.
Snapshot seek157.3 /171.15 /174.3ns;0B/0alloc all.
Repeated iterator229.25 /227.85 /231.55ns;about113B/2alloc.
Durable sync4416817.5 /3721370.5 /3749261.5ns;4771/4625.5/4627B.
Current vs pre-sprint: Get-1.11%, batch-0.41%, seek+10.81%, iterator+1.00%,
durable-15.11% (storage-sync sample noisy). Current Get+36.44% vs recent main.
All raw metrics, including custom WAL/phase counters, remain in logs.

Seek remains above the original5% regression guard. Batch adds2174B and4-5alloc,
violating the original no-added-allocation guard despite inconclusive flat time.
Get's allocation is removed, but its time is only comparable with pre-sprint and
regresses vs recent main. These are OPEN obligations; no mark is silently waived.
No M0/Badger, Dgraph, native-prune, whole-sprint or general performance acceptance.
Structural native production publication/batching remains the critical path;
avoid another allocation tuning or qualification loop before integration.
All repository-required per-PR and final graph gates remain binding.
