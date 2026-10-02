# Actual policy, callers and change audit

Applicable policy inventory at measured9631f9d: Git tracks AGENTS.md,
TreeDB/AGENTS.md and HashDB/AGENTS.md. Root and TreeDB apply; no nested
valuelog/docs/cmd policy exists. CONTRIBUTING.md, TreeDB API contracts,
write-path/durability, recovery/verification and persistent value-log lifecycle
were inspected. The issue4891 completion contract and current graph executor
skill/worker/dependency references apply. TreeDB permits standalone package
captures with documented benchmark names/artifact format/reproduction in both
cmd READMEs; this PR adds those short references and changes no parsers or
unified-bench profile-dir behavior.

The only production changes are the assigned valuelog append/mapping/cache
boundary. Actual mergedD a217 tree equals reviewed provisionaldc86 tree.
D reader.go decoder blob remains unchanged. The subsequent six-line dead-cap
repair was checked independently as a mechanism, then tested red/green across
ReadUnsafe, ReadUnsafeTo and ReadAppend. This advice is not independent final
PR/performance acceptance; the coordinator owns that gate.

Public Get: public.go1861 selects caching.Get or backend.Get. Cached memtable
pointers use the append reader; flushed/reopened backend reads use snapshot
Get -> tree.Get -> GetAppend -> appendPointerValueForKey -> value reader
Set.ReadUnsafeAppend/ReadAppend -> File.ReadAppend. Manager.ReadAppend resolves
the same File boundary. Tree.Get preserves exact owned output or trims excess
capacity; DB snapshot acquisition keeps its existing generation/segment pins.
No caching/backend/tree/options source changes are included.

File.ReadAppend first uses an existing mapping. Only a miss calls existing
sealed-lazy eligibility/budget machinery, then retries mapped append. Successful
mapped hot calls add no Stat/eligibility lock. Current writable policy, closure,
map count/bytes, read integrity and failure fallback remain existing rules.
The shared eligibility helper now rejects a fresh dead-map-cap condition after
that miss: growth cannot safely happen while old unsafe views remain pinned.
No cached denial or manager count/byte denial counter is added. Live effective
cap changes permit recovery, and nil current mapping is not rejected solely
by an old retained count. Low-level remap guard remains in place.

Grouped hits match full file-local start, verification mode, K, raw length,
offsets and sub-index under the existing slot lock. Raw bytes copy directly
into the final owned destination while locked; caller prefix/capacity is
preserved. Insufficient capacity allocates the exact final length. Template
hits keep a private encoded copy, unlock, resolve definitions and append the
decoded payload; resolver eviction/recycling cannot invalidate encoded input.
No borrowed cache data escapes. readTo's prior template ownership/usedDst
semantics remain unchanged. Leaf payload decoding, dictionary decoding,
checksums and decoder bounds are unchanged.

Owned nil mapped reads now share existing entry/raw-frame/raw-byte admission.
Per-file shards, entry policy, raw-length reservation, eviction/pool ownership,
manager raw budget and close clearing remain unchanged. Raw-length budgets are
not physical backing-capacity budgets; existing pooled reuse may retain larger
arrays. Explicit heap, active mmap and setup-inclusive RSS evidence qualify
the measured fixture only. No default threshold/cache size/format/API changes,
GC weakening, pin changes or new zero-copy API are included.

Correctness evidence binds original full race/lifecycle proofs to their actual
runtime identities, and focused repaired race tests to the six-line helper
change. Both required red mechanisms were observed before repair. Full broad
suites were not repeated for docs-only evidence commits; the coordinator owns
exact-final-head CI and review. Failed Mac ENOSPC, initial bad constructor
validator, naive physical-identity clone failure and the first failed timing
packet remain explicitly typed/retained instead of being called green.

Final qualification uses only merged predecessor source, identical public
fixture bytes and matched benchmark source. Private physical index rebind is
supported setup outside timing; the frozen input remains unchanged. All runtime
and harness identities are frozen before capture; final documentation/evidence
edits do not affect those Go sources. The final handoff must record actual
final Git SHA, complete diff scope and zero in-flight writers; no worker CI
polling/merge or hosted model request occurs.
