# Baseline runtime-contract source proposal

This source-only update follows provisional H synchronization of base
`b72ae79285168e0abffd3f610199f36199682c97` with exact reviewed T stage
`98c8a15077d50ebac6ab59ed7a2214aff56b16a9`. H remains provisional. Review,
published/landed-source proof, fresh host-specific manifests/preparation, and
retained decisions remain separate gates. No native build, Go test, benchmark
or preparation was run for this source update. The existing commit hook ran
`go fmt ./...` without changing Go source bytes.

[The contextual patch](baseline-runtime-full-freeze.patch) applies to an
external copy of the synchronized canonical H test/capture files, whose exact
hashes are in [canonical-source-hashes.json](canonical-source-hashes.json).
Patch SHA256:
`351a157d942ac7b77459dad28397edabe78f5d6b169ff599c5e33da729879cf5`.
[The replacement inventory](source-hashes.json) pins all seven external inputs
except the host-specific compiler. The patch reconstructs the two Go tests,
two amended Python capture helpers, preparation helper, and strict source guard
checker. Reuse the unchanged canonical memory overlay generator alongside it.

Only external memory-capture bytes changed from the historical accepted
proposal. The four T runtime hunks define fixed GOMAXPROCS=2,
GOMEMLIMIT=2GiB, GOGC=100, empty GODEBUG/GORACE, GOTRACEBACK=single,
GOWORK=off, GOENV=off, GOFLAGS empty, and GOTOOLCHAIN=local; forward
GOTMPDIR as declared path metadata; reject nonpilot freezes missing/changing
those fixed controls; and add focused runtime-negative selfchecks.
The canonical H observation adapter retains `filter_bytes` and `extra_stats`;
the baseline adapter retains those boundaries while requiring filter-off and
complete absence of negative-filter stats. No fake zero counter, production
backport, fixture/query/CRC/timing/placement change, or schema redesign was added.

The unchanged baseline Go source hashes are T
`68599f74579bcb0f5dba0155a70120ec2a480185044f0edc09cca930191e4508` and H
`84d0c67576d3e3396baa5c421bcf6ac6158958787e071d45e808337d148c3736`.
Canonical H memory Python is now
`4a8c7750475ace639c12f15d13000f8c93602c0074d47523c9938b153f0eead3`;
external baseline memory Python is
`3578922871fc0bf80f5d83c44e68869f24b17f3a19a49119221f4d2bb87127bf`.
Preparation and quicksilver helpers receive the runtime gate through the
existing memory helper; they require no redesign.

[source-manifest.template.json](source-manifest.template.json) is a source-only
Mac-path example, not an accepted product staging or preparation. It retains
the separately labeled prototype `canonical_head=7e1...`, the exact original
product HEAD/trees and canonical Go pins. It uses actual synchronized canonical
addition hashes and reviewed external replacement hashes. Its existing Mac
compiler pin was read from historical source metadata, not newly invoked.
Regenerate resolved paths, actual host/compiler inputs, source additions and
external manifest digest before new preparation. Independently record actual
published/landed H head, Git blob IDs and SHA256 values separately from the
prototype identity; do not replace product identity with harness identity.

Keep historical [baseline documentation](../README.md), patch 87b6, source
manifests, freezes, binaries and streams byte-exact. The historical Mac full
freeze has 1GiB and omitted runtime controls; the later Linux construction
has 2GiB but omits GOTRACEBACK/GORACE. Both fail this stronger full loader and
remain historical construction evidence. Earlier pilots retain their own
identities. Unchanged Go/overlay native compile evidence remains reusable as
historical source portability evidence, not a new environment freeze. Existing
preparation compiles normally; no old-binary import path was added.

Focused Python checks passed for canonical and baseline selfchecks, including
twenty missing/changed runtime-control negatives in each, source hashes and
syntax, actual contextual patch reconstruction, strict guard negatives on an
owned source fixture with mocked Git product identity, old-full runtime
rejection, extra-boundary absence checks and quicksilver complete environment
construction. This is source validation; no Go fixture, build, benchmark or
retained run was executed, and no product identity was fabricated as acceptance.

For the actual campaign, compare complete frozen base environments after
removing only the four declared TREEDB_MEMORY controls. Use identical resolved
TMPDIR/GOTMPDIR, home, path, cache/module and compiler settings across compared
products, and record actual DB filesystem/device/mount before/after collection.
Quicksilver adds its four declared workload controls and verifies the complete
process environment/hash. A's repaired capture has its own complete-environment
freeze; this bridge does not add A to the baseline input allowlist.

Review the source proposal, prove actual landing, regenerate original/control/
final host-specific manifests/freezes, then obtain the coordinator's exclusive
retained campaign grant. Required remaining evidence includes repeated fresh
processes, noise/spread acceptance, owner/placement/correctness observations,
and successful native offline compaction/GC and old-generation pins. No
performance, cold-device, whole-RAM, replacement-equivalence, or concurrent
read-during-update claim follows from this source update.
