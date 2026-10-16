# Original arena lookup admission

The full PRIMARY constructor audit f855f1f9 identifies actual work-bearing
paths that still call plain a.slot/a.valid and therefore traverse the RAM root
bucket without per-link admission. A source-derived L alone does not enforce
the finite caller's work limit at those reads.

Root temporarily owns only TreeDB/internal/primaryarena/arena.go,
read_root_v6.go and a new arena_lookup_admission_v6_test.go, explicitly released
by native_prerequisite. Root will return all three before PRIMARY Growth edits.
No runtime, Go tests, Git/index mutation or freeze is authorized by this source
subtask. Existing staged source remains intact.

Reuse existing slotWithWorkV6/validWithWorkV6 under the same original arena.mu
in Acquire, readRootCertificateV6, AcquireAndDetachReadRootConstructionV6 and
AcquirePromotionCustodyV6. Propagate a lookup budget refusal as ready=false,
nil error before reference, construction-alias, certificate or counter changes.
Promotion resolves all four original operands and validates multiplicity before
incrementing any edge; its local fixed reservation remains, then each RAM
operand pays its original table/link work. Physical bank operands keep the
existing O(1) route. Nil-work callers use the original direct lookup unchanged.

If readRootCertificateV6 must create its original missing certificate, use
AddWithOrdinalWork on the same metadata owner before allocation/attachment.
The selected native path already has a certificate, but an optional finite
policy must not hide the fallback growth. No substitute owner/index or recovery
representation is introduced.

Focused controls must show a quota covering only the old fixed prefix refuses
before effects, successful admitted lookup uses the actual bank/certificate,
promotion's second RAM lookup can refuse without partial acquisition, and
nil-work ordinary behavior stays intact. Existing tests that assumed a four-
record promotion included uncharged lookups need their success allowance
updated while retaining explicit under-admission and invalid-operand assertions.
