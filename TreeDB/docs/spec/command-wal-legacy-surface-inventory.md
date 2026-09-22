# Command-WAL Legacy Surface Inventory

Status: M0 inventory and guardrail contract for issue
https://github.com/snissn/gomap/issues/3613 under parent
https://github.com/snissn/gomap/issues/3612.

Audit base: `origin/main` at
`626163f80649fecf2dc40d8429f921b931c420c6`.

TreeDB is pre-alpha. Public APIs and on-disk formats may change. That means the
command-WAL cleanup stack should prefer a clear current contract over preserving
ambiguous legacy cached-WAL compatibility.

## Vocabulary

| Term | Meaning | Current public surface |
| --- | --- | --- |
| command-WAL | Current durable write log for public TreeDB write handles. Public profiles are `command_wal_durable` and `command_wal_relaxed`. | Allowed and preferred. |
| checkpoint-only benchmark | Unsafe no-command-log ceiling used to measure storage/index cost without a per-write log. | Allowed only when explicitly marked benchmark-only and unsafe. |
| legacy cached redo journal | Old cached-layer redo log controlled by `DisableWAL` / `disableJournal` style flags and generic WAL-on/off durability modes. | Must be removed from public/current guidance or quarantined as internal/forensic compatibility. |
| historical benchmark profile | Old benchmark labels such as `fast`, `wal_on_fast`, `legacy_wal_durable`, `legacy_wal_relaxed_fast`, and `no_wal_fast`. | Historical archives may mention them. Current runbooks must label them as legacy, compatibility, or cross-DB benchmark presets, not public TreeDB server profiles. |

## Search Inventory

The M0 audit used:

```sh
rg -n 'DisableWAL|disableJournal|DurabilityWAL|ProfileLegacy|ProfileNoWAL|ProfileFast|ProfileWALOnFast|legacy_wal|no_wal_fast|wal_on_fast' TreeDB cmd internal docs -g'*.go' -g'*.md'
```

Scoped counts from the audit base:

| Surface | Count | Scope | Owner |
| --- | ---: | --- | --- |
| `DisableWAL` / `disableJournal` | 109 | non-test Go under `TreeDB`, `cmd`, `internal` | #3615 and #3616 |
| `DurabilityWALOffRelaxed` | 40 | non-test Go under `TreeDB`, `cmd`, `internal` | #3614, #3615, #3616 |
| legacy profile constants/usages | 45 | non-test Go under `TreeDB`, `cmd`, `internal` | #3614 |
| generic WAL / legacy profile terms in docs | 310 | Markdown under `docs`, `TreeDB/docs` | #3618 |

The count is intentionally broad. Tests, historical benchmark archives, and
internal compatibility fixtures may retain legacy terms while the cleanup stack
is in flight, but each retained surface needs an explicit classification.

## Current Must-Fix Surfaces

| Surface | Current location | Why it matters | Required disposition |
| --- | --- | --- | --- |
| Public command-WAL opens enter cached mode as `DisableWAL=true` | `TreeDB/public.go` computes `disableWAL` from the WAL-off durability mode or `opts.CommandWAL` and passes it into `caching.Open`. | Command-WAL public writes inherit legacy WAL-off `disableJournal` branches. | #3615 must split command-WAL external durability from unsafe legacy WAL-off cached mode. |
| Cached options expose `DisableWAL` as a mode identity | `TreeDB/caching/db.go` `Options.DisableWAL` and `db.disableJournal`. | The same boolean currently means unsafe no-log mode and command-WAL external durability. | #3615 must rename/split the predicate; #3616 must remove or quarantine old redo-journal writer/replay. |
| Strict sync can checkpoint under `disableJournal` | `TreeDB/caching/db.go` `syncBarrierAfterWrite(sync)`. | Per-write command-WAL sync must not be implemented as hidden backend checkpoint publication. | #3615 and #3617 must prove command-WAL sync counters move without hidden checkpoint fallback. |
| Command-WAL append API requires `disableJournal` | `TreeDB/caching/db.go` `WriteAfterCommandWALAppend`. | The public command-WAL path is coupled to the old disabled-journal flag. | #3615 must make this an explicit command-WAL/external-durability mode or dedicated API. |
| Range batches still have WAL-off bypass branches | `TreeDB/caching/db.go` `writeRangeBatch(sync)` and related stream/direct bypass logic. | DeleteRange/mixed range batches can accidentally use old WAL-off direct paths. | #3615 must guard or fail closed; #3617 must add counter-backed proof. |
| Legacy cached WAL replay is still part of normal open logic | `TreeDB/db/wal_recovery.go` and `TreeDB/db/db.go` `replayWALIntoBackend`. | Old segment families can be confused with command-WAL directories unless default open fails closed or uses explicit compatibility mode. | #3616 must delete, quarantine, or feature-gate legacy cached redo-journal replay. |
| Public/profile names still expose legacy concepts programmatically | `TreeDB/profiles.go`, integration adapters, CLIs, local server wrappers. | Compatibility constants are easy for downstream code to treat as current defaults. | #3614 must narrow public profile/parser/CLI vocabulary to command-WAL plus benchmark-only ceiling. |
| Current docs are mixed | `docs/TREEDB_PROFILES.md`, `docs/contracts/DURABILITY.md`, `docs/TREEDB_RECOVERY.md`, `docs/TREEDB_WRITE_PATHS.md`, `TreeDB/docs/spec/*`. | Docs still teach generic WAL-on/off durability and stale command-WAL range support in places. | #3618 must rewrite current docs and benchmark/report labels after implementation children land. |

## Classification Rules

1. Current public write handles should describe durability in terms of
   command-WAL durable, command-WAL relaxed, or checkpoint-only benchmark.
2. `DisableWAL`, `disableJournal`, `DurabilityWALOffRelaxed`,
   `DurabilityWALOnRelaxed`, `ProfileFast`, `ProfileWALOnFast`,
   `ProfileLegacyWALDurable`, `ProfileLegacyWALRelaxedFast`, and
   `ProfileNoWALFast` are not normal public product vocabulary.
3. A retained legacy term must be one of:
   - internal compatibility code that is being removed or quarantined;
   - a test fixture for old behavior;
   - a historical benchmark archive;
   - a benchmark-only unsafe ceiling with explicit wording;
   - a fail-closed or forensic compatibility path.
4. Historical archives may keep old command lines, but current runbooks must not
   direct new users to choose `fast`, `wal_on_fast`, `legacy_wal_*`, or
   `no_wal_fast` as TreeDB server profiles.
5. The command-WAL cleanup stack must not remove the command-WAL itself or the
   internal commit-log package while command-WAL still depends on it.

## Child Issue Ownership

| Child | Owns |
| --- | --- |
| #3614 | Public profile constants/parsers, user-facing CLI flags, local server config, adapter defaults, and compatibility-name rejection tests. |
| #3615 | Cached-layer mode split, `DisableWAL` / `disableJournal` hot-path predicates, sync barrier behavior, command-WAL write API coupling, and WAL-off bypass guards. |
| #3616 | Legacy cached redo-journal writer/replay, old segment discovery, dirty legacy directory fail-closed behavior, and command-WAL replay separation. |
| #3617 | Counter-backed command-WAL public write proof, crash/reopen replay proof, delete-range support/fail-closed proof, and before/after sync microbench evidence. |
| #3618 | Current docs, benchmark runbooks, report labels, downstream validation checklist, and final parent closeout evidence. |

## Guardrail

Current docs may mention legacy WAL terms only with local context that marks the
reference as legacy, compatibility, historical, deprecated, unsafe,
benchmark-only, internal, forensic, rejected, or not a current public TreeDB
server profile. The docs lint tests enforce this for the current public docs
listed in the test. If a new public doc needs to explain a legacy term, it must
use that context or link here.

Future implementation PRs should tighten this guardrail after #3614-#3618 remove
more legacy surfaces. The final parent closeout should include a refreshed
inventory showing that no public/current generic WAL durability surface remains
outside approved historical/internal paths.
