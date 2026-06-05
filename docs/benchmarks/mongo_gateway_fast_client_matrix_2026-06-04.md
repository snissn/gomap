# TreeDB Fast Mongo/Native Client-Shape Matrix — June 4, 2026

This report records the baseline client-shape evidence behind tracker #2335 and
fixes the benchmark contract for follow-up optimization PRs.

## Standard Runner

Use the checked-in wrapper:

```sh
OUT_DIR=/tmp/treedb_fast_client_matrix_$(date +%Y%m%d_%H%M%S) \
  scripts/mongo_gateway_fast_client_matrix.sh
```

The wrapper delegates to `scripts/mongo_gateway_compare.sh` and pins the current
standard shape unless overridden:

- gateway-shaped BSON documents
- `200000` documents
- batch size `1000`
- `16` insert producers
- two secondary indexes (`email`, `city`)
- concurrent `_id` and `email` reads with `16` readers and `100000` operations
- TreeDB profile `command_wal_relaxed`
- TreeDB read state `settled`
- TreeDB maintenance `none`
- TreeDB client modes: `driver`, `driver-command`, `driver-command-raw`,
  `driver-unack`, `raw-wire-tcp`, `native-wire-tcp`, `direct`
- MongoDB client modes: `driver`, `driver-command`, `driver-command-raw`,
  `driver-unack`

## Current Baseline Artifact

Current profiled artifact root from the tracker setup:

```text
/tmp/treedb_client_shape_profiles_20260604_152750
```

Primary files:

```text
/tmp/treedb_client_shape_profiles_20260604_152750/summary.tsv
/tmp/treedb_client_shape_profiles_20260604_152750/pprof_digest.md
```

Selected rows from that artifact:

| mode | load ops/sec | `_id` read ops/sec | `email` read ops/sec | caveat |
| --- | ---: | ---: | ---: | --- |
| `direct_bench` | 299,777 | 2,026,834 | 230,846 | direct TreeDB storage/collection ceiling |
| `raw-wire-tcp` | 283,472 | 276,187 | 65,365 | baseline artifact predates raw-wire secondary-index find coverage |
| `driver-command-raw` | 276,503 | 124,961 | 70,215 | benchmark-side raw command construction must stay labeled/prebuilt |
| `native-wire-tcp` | 271,948 | 115,712 | 66,917 | native protocol, same collection/index hot path |
| `driver-command` | 253,315 | 130,387 | 69,213 | bypasses part of ordinary CRUD helper overhead |
| `driver` | 228,906 | 115,262 | 62,586 | ordinary Mongo driver path |
| `driver-unack` | 233,169 | 114,026 | 68,454 | not a durable/visibility-equivalent default baseline |

## Interpretation Contract

- Do not compare different client modes as product wins unless the report says
  the client and acknowledgement semantics match.
- `direct` rows are TreeDB storage/collection ceilings, not Mongo protocol rows.
- Raw command/document construction cost is either prebuilt before timing or
  must be labeled as benchmark-side cost.
- TreeDB settled load rows include foreground insert work plus drain/flush. New
  raw JSON phase metrics expose `foreground_duration_ms`,
  `settled_drain_duration_ms`, and `settled_drain_included` when drain is part
  of the phase.
- Follow-up hot-path PRs must use identical baseline/candidate commands,
  fixture shape, hardware/context, and profile/allocation evidence.

## Smoke Validation

For a quick harness check without a full matrix:

```sh
OUT_DIR=/tmp/treedb_fast_client_matrix_smoke \
DOCS_LIST=1000 INDEXES_LIST=0 \
TREEDB_CLIENT_MODES="driver direct" \
MONGO_CLIENT_MODES="driver" \
CONCURRENT_READS=1000 TIMEOUT=5m \
  scripts/mongo_gateway_fast_client_matrix.sh
```

This smoke still requires MongoDB availability according to `MONGO_MODE`.
