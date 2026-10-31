# Final #5335 evidence

This directory publishes the actual final assembler projection and selected raw
phase/control/lifecycle evidence for [the final-source report](../ycsb_mongodb_gomap_final_2026-10-30.md).
The report measures GoMap source `8bea490ad5a2d89a14f02f428b4283579a6dd864`
against actual MongoDB 8.2.7. The identical GoMap tree landed in #5348 as
`dd0e3ffa25a0d39daaafa4499800c8388cdbb2d9`; later documentation commits do
not relabel the measured source.

## Contents and acceptance

- `report.json`: original complete machine-readable assembler output, including
  all selected samples, six paired rates, configuration/runtime, lifecycle,
  original Close and maintenance intervals, snapshot digests and artifact paths.
- `REPORT.md`: original human-readable pair, READ/UPDATE tail, duration and runtime
  tables. INSERT/TOTAL full latency rows remain in `report.json`.
- `attempt-statuses.json`: twelve selected sample slots plus all thirteen actual
  attempts, the original rejected G2, both packet histories and original unstarted
  requests. A rejected attempt is never a valid throughput sample.
- `COMPLETE.json`: original offline-projection receipt; it deliberately retains
  its separate merge/review acceptance wording.
- `raw-phase-evidence.tar.gz`: 9,093 regular members, 349,153,146 expanded bytes;
  compact raw load/run output, original producer Close/error/launch/cleanup,
  manifests, runtime/owner checkpoints, sentinel, lifecycle/control/source/build,
  failure/continuation and qualification receipts. No links or automatic extraction.
- `index.json`: archive digest, member byte/digest inventory, original custody
  paths, twelve declared slots, all thirteen attempts and exact historical source
  relocations. Historical helper acceptances remain historical, not current proof.
- `omissions.json`: explicit private/cache/DB/ELF/typed-payload exclusions and
  original content-digest authorities. Full original DBs, ELFs and typed states
  remain in their documented local custody; this archive does not publish them.
- `VERIFICATION.json`: actual unchanged local verifier result, including exact
  source-byte identity for selected members, cardinality and report arithmetic.
  Its legacy `actual_attempts=12` field counts declared sample slots. Actual
  attempts are **13**, explicitly retained in `index.all_attempts` and the ledger.
- `PORTABLE-VERIFICATION.json`: actual archive-only member/digest/cardinality
  audit without original local paths, plus a bounded publication pattern check.
- `FINAL12-TERMINAL-CUSTODY-AUDIT.json`: actual terminal cleanup evidence for all
  thirteen attempts, 501 owned/coordinator PID/start identities and 24 containers.
- `PUBLIC-CONTROL-SCOPE.md` and `HISTORICAL-COMPARABILITY.md`: source-bound scope
  of the separately executed original792 ACK/crash controls and historical context.
- `FINAL-DELIVERY-ACCEPT.json`: ROOT accepts the data/evidence packet, preserving
  final documentation PR review/CI and unresolved parent performance obligations.
- `SHA256SUMS`: checksums of the published artifacts (excluding this explanatory
  README and the checksum file itself).

All selected loads/runs have zero operation errors and successful maintained
lifecycle/Close/runtime checks. Results are mixed: median paired relaxed ratio
1.372250; durable-B 0.591905. Differing durable requests, short timing, census cache
warming, resource asymmetry and random-state limitations remain in the report.
No failed gate is waived and no general COW speedup is inferred. Earlier packet,
parser, resource, build and numerical cost failures retain their original
identities; three later failed offline archival preparations are also preserved
locally and referenced by the archived current helper/source-repair receipts.

## Verify the published bytes

From this directory, `sha256sum -c SHA256SUMS` checks the published files.
The following Python 3 code verifies the archived members against the public
index without extracting anything or accessing the original absolute local paths:

```python
import hashlib, json, pathlib, tarfile

root = pathlib.Path('.')
index = json.loads((root / 'index.json').read_text())
archive = root / index['archive']['path']
def digest(stream):
    h = hashlib.sha256()
    for block in iter(lambda: stream.read(1024 * 1024), b''):
        h.update(block)
    return h.hexdigest()
with archive.open('rb') as stream:
    assert digest(stream) == index['archive']['sha256']
entries = {entry['archive_path']: entry for entry in index['archive']['members']}
assert len(entries) == len(index['archive']['members'])
seen = set()
with tarfile.open(archive, 'r:gz') as bundle:
    for member in bundle:
        assert member.isfile() and not member.issym() and not member.islnk()
        assert not member.name.startswith('/')
        assert '..' not in pathlib.PurePosixPath(member.name).parts
        assert member.name in entries and member.name not in seen
        seen.add(member.name)
        entry = entries[member.name]
        assert member.size == entry['bytes']
        with bundle.extractfile(member) as stream:
            assert digest(stream) == entry['sha256']
assert seen == set(entries)
assert len(index['attempts']) == 12
assert len(index['all_attempts']) == 13
assert len(index['rejected_attempts']) == 1
print('PASS:', len(seen), 'members; 12 selected slots / 13 actual attempts')
```

This portable audit proves selected archived-byte integrity, not omitted-payload
contents, independent crash certification or statistical significance. The stronger
actual local verifier additionally compared selected original source bytes and
retained omission authorities; its original receipt is preserved unchanged.

## Reproduction sources and phase boundaries

The archive contains the actual original sealed `packet/` and `prior/packet/`
controllers/configuration, their requests and consumed statuses, every selected
`cells/<class>-pair<N>-<arm>/request.json`, output manifests and launch receipts.
The rejected original G2 is under `prior/rejected/`. The first two valid samples
retain original packet identities; the next ten use the corrected observer with
the original global origin/budget. Do not replay consumed controllers or silently
reuse their output directories.

For a fresh reproduction, use the pinned source/build/client/image identities
and exact request/phase argv in that inventory with fresh output/database paths.
The maintained [reference lifecycle contract](../../../scripts/ycsb_mongodb_reference.md)
and checked-in reference preparation/run scripts describe the explicit MongoDB
route; the ordinary TreeDB shell still refuses direct `TARGETS=mongodb`.
The current timed Mongo producer is **unmodified** merged `snissn/go-ycsb`
`3c8148495b8d4490cf0ec30e9eb92d14a7a71bac`, ELF
`2237d8aeec309b9b4b335b348234d9829594c9101043144a944d9f2868fbc95b`.
Native compatibility overlays and June workloads are separate evidence.

Keep all 100k load/10k run/16 workers/95:5/ten binary 100-byte fields, original
TOTAL timing and actual checked Close, fresh fixtures, census boundary and the
explicit relaxed versus durable-B concerns/profiles. Durable-B's same-owner
public j:true sentinel is outside TOTAL. Archive phase/Close/maintenance records
retain those distinct durations. Actual child GOMAXPROCS absence/effective value 12,
inherited affinity/quota, COW activation and asymmetric MongoDB/GoMap resource
caps belong to this execution; a new host or build is a new comparison.
