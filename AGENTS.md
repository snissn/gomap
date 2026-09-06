# Working in gomap

TreeDB is the main product: a pre-alpha persistent Go storage engine with a
copy-on-write B+Tree, persistent value log, command-WAL recovery, collections,
secondary/vector indexes, and native-wire and Mongo-compatible servers.
HashDB is the older comparison/experimental engine. See [README](README.md)
for public usage and [CONTRIBUTING](CONTRIBUTING.md) for the development workflow.

## Code map

- `TreeDB/`: public engine API; `db/`, `caching/`, `node/`, `pager/` and
  `internal/` implement storage, publication, recovery and value-log machinery.
- `TreeDB/collections/` and `TreeDB/documentservice/`: document/index behavior
  and service operations. Follow the actual handler; not every route is shared.
- `TreeDB/nativewire/`, `TreeDB/mongo_gateway/` and
  `cmd/treedb-native-server/`: protocol handlers, clients and server startup.
- `HashDB/`: hash engine. `kvstore/` and `TreeDB/integration/`: adapter boundaries.
- `cmd/`, `scripts/`: tools and benchmark entry points; `.github/workflows/`:
  executable CI policy. Dated benchmark reports are evidence, not API contracts.

## Critical boundaries

- TreeDB's value log is persistent storage, not a disposable WAL. Never delete
  segments by age; respect reachability, retained readers and rewrite publication.
  HashDB's storage layout is different.
- Preserve the selected profile's acknowledgement and recovery guarantees.
  `Flush` is not a durability boundary. Use the
  [profile contract](docs/TREEDB_PROFILES.md), not benchmark defaults, for servers.
- APIs/formats are pre-alpha; intentional format breaks need updated specs and
  reopen/recovery coverage, not speculative migration infrastructure.
- Preserve permissions, user work and release gates. Do not merge or deploy
  without authorization, or call a change verified while required checks fail.

## Work and verification

Keep one owner responsible for the observable outcome and integration. Trace
only the affected path, reuse existing mechanisms, and keep the change bounded.
See [completion and review](CONTRIBUTING.md#completion-and-review) for evidence,
risk-based checks, delegation and blocker handling.

Run commands from the repository root with the Go version in `go.mod`:

- Native-wire pilot: `make check-nativewire` (build, vet, uncached package tests).
- Workflow changes: `make workflow-check`; documentation: `make docs-check`.
- Go changes: format changed files, run affected package tests, then broaden as
  risk requires. `make test` and `make vet` cover the root module once;
  `make test-race` is a focused subset, not the entire race matrix.

## Load guidance when relevant

- Storage changes: read [TreeDB/AGENTS.md](TreeDB/AGENTS.md) and the linked specs.
- HashDB changes: read [HashDB/AGENTS.md](HashDB/AGENTS.md).
- Performance/profiling: read the
  [profiling workflow](CONTRIBUTING.md#benchmark-profiling-workflow) and the
  affected tool's README before running its harness.
- Deeper review: select the relevant [review playbook](review-prompts/README.md).
  These are task-specific guidance, not a mandatory sequence of reviews.
