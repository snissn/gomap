#!/usr/bin/env python3
"""Render the documentation from validated fixed-attempt summaries."""
import argparse
import json
from pathlib import Path

STEM = 'readme_ycsb_mongodb_macos_2026-11-12'
MODES = ('driver', 'driver-command', 'driver-command-raw', 'raw-wire-tcp', 'native-wire-tcp')
NAMES = {
    'driver': 'TreeDB / Mongo driver CRUD',
    'driver-command': 'TreeDB / Mongo driver command',
    'driver-command-raw': 'TreeDB / Mongo driver raw command',
    'raw-wire-tcp': 'TreeDB / Mongo BSON raw wire over TCP',
    'native-wire-tcp': 'TreeDB / native wire over TCP',
}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', required=True, type=Path)
    parser.add_argument('--repo', required=True, type=Path)
    args = parser.parse_args()
    root, repo = args.root.resolve(), args.repo.resolve()
    data = json.loads((root/'derived/summary.json').read_text())
    assert data['selected_count'] == 47 and data['invalid_retained_count'] == 1
    assert len(data['attempt_ledger']) == 79
    groups = {(g['suite'], g['label'], g['target']): g for g in data['summaries']}

    def group(suite, label, target):
        g = groups[(suite, label, target)]
        assert g['n'] == 3 and g['complete_three_repetitions']
        return g

    def number(x, digits=0):
        return f'{x:,.{digits}f}'

    def median(d, digits=0):
        return number(d['median'], digits)

    def spread(d, digits=0, divisor=1):
        return f"{number(d['median']/divisor,digits)} [{number(d['minimum']/divisor,digits)}, {number(d['maximum']/divisor,digits)}]"

    def metric(suite, label, target, phase, key='ops_per_sec'):
        return group(suite, label, target)['metrics'][phase][key]

    relaxed_ratio = data['ycsb_paired_ratios']['relaxed']
    assert relaxed_ratio['complete_three_pairs'] and relaxed_ratio['valid_pair_count'] == 3
    incomplete_ratio = data['ycsb_paired_ratios']['different-durable-requests']
    assert incomplete_ratio['ratio'] is None and incomplete_ratio['valid_pair_count'] == 2
    assert len(data['incomplete_groups']) == 1 and data['incomplete_groups'][0]['target'] == 'mongodb'
    ratio = relaxed_ratio['ratio']['median']
    yrows = []
    for label, title in [('relaxed', 'Relaxed: both `w:1`, `j` absent'),
                         ('different-durable-requests', 'Different durable requests/profiles; incomplete')]:
        gm = group('ycsb', label, 'gomap')['metrics']['run']['TOTAL']['ops_per_sec']
        mongo = median(group('ycsb', label, 'mongodb')['metrics']['run']['TOTAL']['ops_per_sec'], 1) if label == 'relaxed' else 'N/A (2/3 valid)'
        yrows.append(f'| {title} | {mongo} | {median(gm,1)} | {ratio:.3f}x |' if label == 'relaxed'
                     else f'| {title} | {mongo} | {median(gm,1)} | N/A |')
    ytable = '\n'.join([
        '| ACK / profile class | MongoDB 8.2.7 ops/s | GoMap Mongo gateway ops/s | Median paired ratio |',
        '| --- | ---: | ---: | ---: |', *yrows])

    shape_rows = []
    for mode, target, title in [(m, 'gomap', NAMES[m]) for m in MODES] + [('driver-command-raw', 'mongodb', 'MongoDB / driver raw command')]:
        phases = group('shape', mode, target)['metrics']
        load, ids, emails = [phases[p] for p in ('load_insert_many', 'concurrent_id_find_one_r16', 'concurrent_email_find_one_r16')]
        shape_rows.append(f"| {title} | {median(load['ops_per_sec'])} | {median(ids['ops_per_sec'])} | {median(emails['ops_per_sec'])} | {median(ids['p95_us'])} | {median(emails['p95_us'])} |")
    shape_rows.append('| TreeDB direct collection API | N/A | N/A | N/A | N/A | N/A |')
    shape_table = '\n'.join([
        '| Access path | Load docs/s | `_id` reads/s, r16 | `email` reads/s, r16 | `_id` p95 us | `email` p95 us |',
        '| --- | ---: | ---: | ---: | ---: | ---: |', *shape_rows])

    sweep_rows = []
    for count in (0, 1, 2):
        gm, mo = [group('sweep', 'indexes'+str(count), t) for t in ('gomap', 'mongodb')]
        vals = [median(g['metrics'][p]['ops_per_sec']) for p in ('load_insert_many', 'concurrent_id_find_one_r16') for g in (gm, mo)]
        vals += [number(g['allocated_bytes']['median']/2**20, 2) for g in (gm, mo)]
        sweep_rows.append('| '+str(count)+' | '+' | '.join(vals)+' |')
    sweep_table = '\n'.join([
        '| Secondary indexes | TreeDB load docs/s | MongoDB load docs/s | TreeDB `_id` r16 ops/s | MongoDB `_id` r16 ops/s | TreeDB allocated MiB | MongoDB allocated MiB |',
        '| ---: | ---: | ---: | ---: | ---: | ---: | ---: |', *sweep_rows])

    link = f'docs/benchmarks/{STEM}.md'
    sections = {}
    sections['YCSB Server Workload'] = f'''### YCSB Server Workload

November 12, 2026 local Mac snapshot: native macOS arm64 client and GoMap,
Docker Desktop Linux arm64 MongoDB 8.2.7. The public Mongo BSON API uses the
same external `go-ycsb` binary, 100,000 records, 10,000 operations, 16 workers,
and 95% reads / 5% updates. Cells are medians of three fresh valid fixtures.

{ytable}

The second class requests MongoDB `w:1,j:true` versus GoMap
`command_wal_durable` with `w:1`, `j` absent, for both load and run. One MongoDB
reopen failed; its throughput is excluded and the paired comparison is incomplete.
All selected YCSB runs have zero operation errors and checked original Close
receipts. The relaxed paired ratios range from {relaxed_ratio['ratio']['minimum']:.3f}x
to {relaxed_ratio['ratio']['maximum']:.3f}x. These short, census-warmed runs on a shared laptop are scoped local
evidence. Full ranges, tails, every attempt, topology and validation limits:
[November Mac benchmark report]({link}).
The [October Linux qualification](docs/benchmarks/ycsb_mongodb_gomap_final_2026-10-30.md)
and [June history](docs/benchmarks/ycsb_latest_main_2026-06-03.md) remain separate.

'''
    sections['TreeDB Mongo Gateway Client-Shape Workload'] = f'''### TreeDB Mongo Gateway Client-Shape Workload

November 12, 2026 on the same shared Mac: 200,000 gateway-shaped BSON documents,
batch 1,000, 16 insert producers, two secondary indexes (`email`, `city`),
prebuilt documents, TreeDB `command_wal_relaxed`, settled reads and 100,000
reads per kind with 16 readers. Each cell is the median of three fresh runs;
latency cells are medians of per-run p95 values.

{shape_table}

MongoDB runs in Docker Desktop's Linux VM; native wire is a separate TreeDB
protocol. Direct API index setup is currently rejected under this command-WAL
profile, so its old ceiling is unavailable. The obsolete unacknowledged-write
row is removed because the current gateway rejects `w:0`. All selected runs
passed the independent full reopened-data checks; the existing macrobenchmark's
original deferred cleanup return is unobserved.
[November report, ranges and raw evidence]({link}).

'''
    sections['Mongo API Full-Sweep Workload'] = f'''### Mongo API Full-Sweep Workload

November 12, 2026 on the same Mac and Docker topology, 100,000 gateway-shaped
BSON documents, batch 1,000, 16 producers, matched `driver-command-raw` clients,
TreeDB `command_wal_relaxed` and settled reads. Medians of three fresh pairs
per row; the additional `age_1` range index exists even in the `0` row.

{sweep_table}

Allocated bytes cover the entire stopped DB after the full read/range/update
sweep and untimed TreeDB checkpoint or MongoDB compact, including journals
and persistent value logs. All selected runs passed the full reopened-data
checks; the original macrobenchmark cleanup-return limitation above applies.
[November report with every phase, ranges, storage basis and raw evidence]({link}).

'''
    readme = (repo/'README.md').read_text()
    for heading, replacement in sections.items():
        start = readme.index('### '+heading+'\n')
        end = readme.find('\n### ', start+4)
        assert end != -1
        readme = readme[:start]+replacement.rstrip()+'\n'+readme[end:]
    (repo/'README.md').write_text(readme)

    lines = [f'''# Local Mac YCSB and MongoDB benchmark refresh — November 12, 2026

This snapshot replaces three README tables using fixed-source measurements on
the user's Mac. It is a documentation/evidence refresh for
[#5407](https://github.com/snissn/gomap/issues/5407). No production or maintained
benchmark code changes. The requested PR stops before merge.

There are **{data['selected_count']} valid retained samples and {data['invalid_retained_count']} invalid retained sample** across
48 planned slots: YCSB 11/12, client shape 18/18, full sweep 18/18. All 48 slots
were attempted once. No failed slot was retried. The journaled MongoDB YCSB
comparison remains incomplete. Smoke accounting is separate: 29 valid and
two invalid attempts, including an untimed validator-query mismatch and the
unsupported direct API setup. The archive retains all 79 attempts and their
original source/freeze versions.

## Source, host and timing boundary

| Input | Observed value |
| --- | --- |
| GoMap and maintained harness | `2590d1e9e443c3f968303ce91b3c82933a714f12` |
| External client | clean `snissn/go-ycsb@3c8148495b8d4490cf0ec30e9eb92d14a7a71bac` |
| Toolchain | Go 1.26.10, matching resolved GOROOT/compiler, CGO enabled; native darwin/arm64 |
| YCSB build tags | `libsqlite3,sqlite_omit_load_extension`; no compatibility overlay |
| Host | Mac15,13, 8 physical/logical CPUs, 16 GiB RAM; macOS 26.2 (25C56), Darwin 25.2.0 arm64 |
| Initial storage/power snapshot | local APFS `/dev/disk3s5`; about 21.3 GiB free; AC power, battery 100% |
| Docker | Desktop 27.5.1; Linux aarch64 6.12.5-linuxkit, 8 CPUs, 8,217,858,048 bytes VM memory |
| MongoDB | 8.2.7 arm64; immutable image ID and digest below; public buildInfo checked per case |
| Native Go runtime | `GOMAXPROCS` unset; actual runtime value not independently observed |
| Frozen input manifest SHA-256 | `{data['frozen_manifest_sha256']}` |

MongoDB image ID:
`sha256:b8b32a56a1ddc4c278d68124a8f0203663018f62828cadaa995a6cd595f6c9a0`.
Repository digest:
`mongo@sha256:d65a15bd109e970d7dab5496d0a4254705793b8a5c49e344aa6126d46b0e4630`.
The collection-end snapshot also reports AC power, with the battery at 86%
and charging. Power was not continuously monitored.
The tag lookup was unreliable in the local Docker API, so launch used the
observed immutable image ID. The final manifest records all six binary hashes,
build info, source tree/blob IDs, Docker resources and the host/power/process
snapshot. The initial Mac YCSB SQLite-extension link failure is preserved as
setup evidence; the successful build adds the supported omit-extension tag.

This was an active shared laptop with Codex/UI applications running, not an
isolated benchmark server. User applications were left running. Task-owned
builds, tests, compression and cleanup were kept outside timed collection;
`caffeinate -i` prevented idle sleep. GoMap and clients execute natively on
macOS while MongoDB executes inside Docker Desktop's Linux VM with a host bind
mount. CPU, memory and filesystem topology are asymmetric. These numbers do
not establish a cross-date speedup or isolate COW's contribution.

All tools were built before the retained runs. Each case starts with a fresh
DB directory. Repetition target order alternates MongoDB/GoMap, GoMap/MongoDB,
MongoDB/GoMap; the client-shape TreeDB mode order reverses in repetition two,
then its MongoDB context row follows each mode set. The exact timestamped
execution order and argv are retained. The five never-started YCSB slots were
completed through a reviewed continuation after the failed reopen; no completed
or failed slot was replayed.

The existing producer timing boundaries were preserved. YCSB TOTAL excludes
original client Close. Macrobenchmark load and operation phases exclude tool
builds, public correctness checks, reopen, storage census and maintenance.
Per-run p95/p99 values remain per-run quantiles; their medians below are not
pooled-request quantiles. Brackets show the full minimum–maximum range of the
three valid runs. Individual valid rates and tails are retained in `summary.json`.

## YCSB server

100,000 records; 10,000 uniform operations; 16 workers; 95% reads / 5% updates;
ten binary fields of 100 bytes; hashed FNV user keys; BSON; no inserts/scans/RMW
in the run phase. The same external MongoDB binding and binary serves both
arms. A complete loaded-data census warms the cache before the short run.

{ytable}

Relaxed uses GoMap `command_wal_relaxed` and both clients request `w:1` with
`j` absent. The second class uses GoMap `command_wal_durable`, `w:1`, `j`
absent versus MongoDB `w:1,j:true`; these differing requests apply to both
load and run. It is not a new durable-B, crash, power-loss or same-request
qualification. The maintained Linux YCSB runtime/ELF/procfs validator cannot
run unchanged on this native Mac; this local direct-CLI evidence is lower
scoped and does not replace the
[October Linux qualification](ycsb_mongodb_gomap_final_2026-10-30.md) or reopen
the closed #5335 qualification graph.

The paired relaxed ratio is the median of the three matched run ratios.
The second class has no paired headline or MongoDB aggregate because its
first MongoDB attempt failed the required reopen gate. GoMap's three valid
durable-profile runs are reported independently. No partial failed-sample
throughput enters a selected cell.

### Valid YCSB samples and tails

All selected operation errors are zero. Rates are ops/s; avg/p95/p99/max are
microseconds from the original YCSB producer. `READ` and `UPDATE` run counts
sum to 10,000; the randomized mix varies per run.

| Request class | Target | Rep | Phase / operation | Count | Ops/s | Avg us | p95 us | p99 us | Max us |
| --- | --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |''']
    for sample in sorted((s for s in data['samples'] if s['suite'] == 'ycsb'), key=lambda s:(s['label'],s['repeat'],s['target'])):
        for phase, operations in [('load', ['TOTAL']), ('run', ['TOTAL','READ','UPDATE'])]:
            for operation in operations:
                m = sample['metrics'][phase][operation]
                lines.append('| '+' | '.join([sample['label'],sample['target'],str(sample['repeat']),phase+'/'+operation,
                    number(m['count']),number(m['ops_per_sec'],1),number(m['avg_us'],1),number(m['p95_us'],1),number(m['p99_us'],1),number(m['max_us'],1)])+' |')
    lines.append('''
### YCSB distributions

| Request class | Target | Load ops/s median [min, max] | Run ops/s median [min, max] |
| --- | --- | ---: | ---: |''')
    for g in data['summaries']:
        if g['suite'] == 'ycsb':
            lines.append(f"| {g['label']} | {g['target']} | {spread(g['metrics']['load']['TOTAL']['ops_per_sec'],1)} | {spread(g['metrics']['run']['TOTAL']['ops_per_sec'],1)} |")
    lines.append('''
### Invalid journaled MongoDB attempt

`runs/ycsb/ycsb-different-durable-requests-p1-mongodb` completed its original
load/run and original Close receipts, first full public data checks, deterministic
fixture and first graceful stop. The new MongoDB container failed during reopen:
exit 133, `OOMKilled=false`; WiredTiger reported ENOENT while syncing
`/data/db/_tmp/spilldb/` and creating `WiredTiger.turtle.set`, then WT_PANIC /
fatal assertion 50853. The diagnostic logs and inspect output are preserved.
The coordinator interrupted the owned runner's dead-server readiness loop;
cleanup removed the failed container and retained the DB until evidence review.
The explicit `failure-disposition.json` records the failure despite Python's
empty `KeyboardInterrupt` string. This cell is invalid and excluded in full.
The other two MongoDB journaled attempts passed; the evidence does not establish
that `journal=true` caused this Docker bind-mount reopen failure.

### YCSB correctness and lifecycle scope

The untimed public validator enumerates every expected FNV key, checks all ten
typed binary fields and their lengths, and rejects missing/extra/duplicate rows.
Canonical typed-BSON digests must match before close and after independent
reopen. A separate 64-record deterministic exact-value write/update fixture
is read back after reopen. MongoDB's fixture uses `w:1`/default even in the
journaled class, so it is not a `j:true` fixture qualification. The randomized
workload check preserves observed state; it cannot prove every acknowledged
random update's intended value or atomicity.

The original YCSB command, DB Close and workload Close are checked at their
producer with phase/build/profile identity. Gateway SIGTERM must exit zero,
covering its joined original Close and server exit; MongoDB stop must be
graceful with container absence confirmed. These are graceful lifecycle checks.
They do not establish crash or failure-semantics equivalence.

## Mongo gateway client shape

200,000 prebuilt gateway-shaped BSON documents, batch 1,000, 16 producers,
two secondary indexes (`email` unique, `city`), full-document point responses,
settled reads, 100,000 reads of each kind at 16 readers. TreeDB uses
`command_wal_relaxed`, BSON and compressed roots; maintenance is `none`.
The MongoDB driver-raw row uses the same counts and client pool limits
(maximum pool 16, maximum connecting 16, minimum pool 0) in the Linux VM.

'''+shape_table+'''

The old direct collection ceiling is unavailable: fixed-source smoke index
setup rejects catalog index mutation under `command_wal_v2` before load.
Its failed attempt and exact error are retained in `smoke-unhinted`. A different
profile was not substituted. The old driver-unack row is removed because
current TreeDB rejects `w:0` before mutation. Native wire is a distinct
TreeDB protocol, not a MongoDB wire compatibility measurement.

### Client-shape phase distributions

Zero-operation placeholder phases are omitted. Rates are ops/s (load is docs/s).
Latency columns are medians of per-run p95/p99 in microseconds.
Load quantiles describe batch driver calls, not single-document latency.

| Target / mode | Phase | Ops/s median [min, max] | p95 us | p99 us |
| --- | --- | ---: | ---: | ---: |''')
    for g in data['summaries']:
        if g['suite'] != 'shape':
            continue
        for phase,m in g['metrics'].items():
            if phase in ('load_insert_many','concurrent_id_find_one_r16','concurrent_email_find_one_r16'):
                lines.append(f"| {g['target']} / {g['label']} | `{phase}` | {spread(m['ops_per_sec'])} | {median(m['p95_us'])} | {median(m['p99_us'])} |")
    lines.append('''
## Mongo API full sweep

100,000 prebuilt gateway BSON documents, batch 1,000, 16 producers,
`driver-command-raw` on both arms, full responses, TreeDB relaxed and settled
reads. Each row adds the `age_1` range index regardless of the displayed
secondary count; count one adds unique `email_1`, count two also adds `city_1`.
Sequential `_id` reads total 100,000, email reads total 100,000 when present,
range reads and updates each total 10,000. Concurrent `_id` reads total 100,000
at each of 1/16/32 workers; concurrent range reads and writes total 10,000
each at each of 1/16/32 workers. Deletes are zero.

'''+sweep_table+'''

### Physical allocation endpoint

Allocation is the sum of macOS `stat.st_blocks * 512` for every file under
the stopped DB directory after the full sweep and untimed maintenance:
TreeDB checkpoint versus MongoDB compact. It includes all redo/command
journals, WAL and persistent value logs. It excludes Docker's VM image,
program binaries, source, caches and the task-owner marker outside the DB.
This is allocated space at that endpoint, not logical size, post-insert-only
size, WAL-excluded size, or identical maintenance semantics. MiB means 2^20
bytes. Per-file logical and allocated bytes are retained.

Maintenance is separately identified by raw `treedb_maintenance_mode=checkpoint`
and `mongo_compact=true`; normal producer completion requires these steps to
succeed. Their individual durations are not separately timed by this existing
harness. No maintenance latency or equivalence is inferred from phase timings.

| Secondary indexes | Target | Allocated MiB median [min, max] |
| ---: | --- | ---: |''')
    for n in (0,1,2):
        for target in ('gomap','mongodb'):
            g = group('sweep','indexes'+str(n),target)
            lines.append(f"| {n} | {target} | {spread(g['allocated_bytes'],2,2**20)} |")
    lines.append('''
### Full-sweep phase distributions

Rates are ops/s (load is docs/s); latency columns are medians of per-run
p95/p99 in microseconds. Exact operations, durations and every per-run tail
are retained in the summary and raw producer JSON.
Load quantiles describe batch driver calls, not single-document latency.
''')
    for n in (0,1,2):
        lines.append(f'''\n#### {n} displayed secondary indexes, plus age range index

| Target | Phase | Ops/s median [min, max] | p95 us | p99 us |
| --- | --- | ---: | ---: | ---: |''')
        for target in ('gomap','mongodb'):
            g = group('sweep','indexes'+str(n),target)
            for phase,m in g['metrics'].items():
                lines.append(f"| {target} | `{phase}` | {spread(m['ops_per_sec'])} | {median(m['p95_us'])} | {median(m['p99_us'])} |")
    lines.append(f'''
## Macrobenchmark validation and limits

All 36 selected macrobenchmark attempts completed the declared phase counts
and independently reopened the full expected document set. The public typed
validator checks IDs, immutable base fields, typed mutation-marker pairs,
zero missing/extra/duplicate rows, declared index metadata where supported,
sample `_id`/email lookups and complete expected city/age query results.
MongoDB preclose/reopen canonical state digests also match. The initial strict
hint query was unsupported by this gateway; the corrected validator uses the
same unhinted public query form as the producer and retains the initial failed
smoke and its old source/freeze. Index metadata and result correctness do not
prove which physical index a query activated.

The existing `mongo_gateway_bench` discards its original deferred
`closeBenchTarget` return. That result is **unobserved** in every shape/sweep
sample; a later successful verifier Close cannot recover it. Raw-command insert
counts are nominal producer batch counts after command Err checks; the complete
public reopened census independently verifies final data. The validator does
not prove every intermediate update's intended result or concurrent schedule.
Verification gateways have separately checked successful original exit/Close,
and external MongoDB containers have checked graceful stop/absence. No claim
of original macrobenchmark cleanup success, crash durability, every ACK,
transaction atomicity or full MongoDB semantics follows.

## Evidence and reproduction

The [{STEM} evidence directory]({STEM}/) contains:

- `summary.json`: deterministic selection, complete three-repetition summaries,
  individual valid sample metrics, excluded slot and all-attempt ledger.
- `freeze.json`: actual final precollection source/runtime/binary/host manifest.
- `raw.tar.gz` and `raw-index.json`: compact raw outputs, original command/Close/
  lifecycle receipts, typed censuses, all failed attempts/dispositions, setup
  logs, smoke source history, helper/recipe source and precollection reviews.
- `aggregate.py`, `render.py`, `verify_archive.py`: analysis and archive checks;
  `census.go.txt` preserves validation-only Go source without adding a repo Go
  package. The archive contains its original `recipe/census.go` filename.

Archive inspection and deterministic aggregation require Python 3.9+ and no
database server or Go build. From the repository root:

```sh
EVIDENCE=docs/benchmarks/{STEM}
python3 "$EVIDENCE/verify_archive.py" --directory "$EVIDENCE" &&
EXTRACT=$(mktemp -d /tmp/gomap-readme-evidence.XXXXXX) &&
tar -xzf "$EVIDENCE/raw.tar.gz" -C "$EXTRACT" &&
python3 "$EXTRACT/recipe/aggregate.py" --root "$EXTRACT" &&
cmp "$EXTRACT/derived/summary.json" "$EVIDENCE/summary.json"
```

A new measurement campaign must build the pinned product and external client
with the recorded toolchain, tags and build commands, copy the archived
validation/runner sources into a fresh task root, and freeze the new absolute
source/binary locations using `recipe/freeze.py --root "$TASK" --repo "$PRODUCT"
--go "$GO" --mongo-image-id <observed-immutable-ID>`. The portable CLI is an
analysis/reproduction convenience; `recipe-history/freeze-original.py` preserves
the actual generator used for this campaign. The generator records host
and binary identities anew; the historical manifest must not be reused or
edited to pass validation. Build `recipe/census.go` with the product module
environment (`go build -o "$TASK/bin/census" "$TASK/recipe/census.go"`).
Then execute each suite once, serially, with the reviewed recipe:

```sh
caffeinate -i python3 "$TASK/recipe/run.py" --root "$TASK" --repo "$PRODUCT" --suite smoke
caffeinate -i python3 "$TASK/recipe/run.py" --root "$TASK" --repo "$PRODUCT" --suite ycsb
caffeinate -i python3 "$TASK/recipe/run.py" --root "$TASK" --repo "$PRODUCT" --suite shape
caffeinate -i python3 "$TASK/recipe/run.py" --root "$TASK" --repo "$PRODUCT" --suite sweep
```

Exact argv/environment/phase receipts are the authority for each historical
sample. A failure stops its suite; retain it and reassess rather than rerun to
obtain a favorable sample. The one reviewed historical continuation fills only
the five never-started YCSB slots. Raw DBs, core dumps, binaries and caches are
excluded from Git. Successful case DBs were released only after required
public/lifecycle validation and a saved validation record.
''')
    report = '\n'.join(lines).rstrip()+'\n'
    report_path = repo/'docs/benchmarks'/(STEM+'.md')
    report_path.write_text(report)

    current_path = repo/'docs/benchmarks/ycsb_mongodb_treedb_current.md'
    current = current_path.read_text()
    old = '''It distinguishes the retained October comparison from historical reports and
records the remaining qualification obligations.

## Final-source MongoDB comparison'''
    new = f'''The latest README snapshot is the [November 12 native Mac report]({STEM}.md).
It retains 11 valid YCSB samples and one failed MongoDB reopen, so the second
request/profile comparison is incomplete. The relaxed row has three fresh
pairs. Native Mac client/GoMap versus Docker Desktop Linux arm64 MongoDB is
lower-scoped local evidence, with checked original YCSB Close and graceful
reopen controls. It is not a pass of the Linux-only runtime validator or a new
durable-B/crash qualification. The same report refreshes the MongoDB client
shape and full-sweep README tables.

The retained October Linux qualification and historical reports below keep
their original source, acceptance and limitations.

## October final-source MongoDB comparison'''
    if old in current:
        current = current.replace(old, new, 1)
    else:
        assert new in current
    current_path.write_text(current)
    print(json.dumps({'report':str(report_path),'readme_sections':list(sections)}))


if __name__ == '__main__':
    main()
