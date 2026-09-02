# Vector-partition qualification evidence for #4027

This directory is the compact review manifest for the final structured
qualification of [issue #4027](https://github.com/snissn/gomap/issues/4027).
The single fail-closed validator returned `qualified` for the exact code and
evidence identities below.

This is **not** the full replay bundle. The raw TreeDB directories, profile
artifacts, transcript-v5 files, truth caches, retained binary, detached source
clone, and command logs remain locally retained under
`/mnt/fast4tb/gomap-4027-qualification-campaign-eed54bc0` and are intentionally
omitted here. `inventory/full-campaign.sha256` binds all 3,549 regular files in
that 5.2 GiB local bundle; `SHA256SUMS` binds this compact manifest.

## Exact identity

| Item | Value |
| --- | --- |
| Base | `03e7a26e56100964f14f603f0248a1a6ccc50a68` |
| Qualified head | `eed54bc0b9ec3b705e9170be26ab069bdc9b9771` |
| Benchmark binary SHA-256 | `c37f432d79a5148bd8397ddd2c9ab705d068b4b55717ec395b7431e7e9f6ea7b` |
| Campaign index SHA-256 | `c20f11bb38898fd0d5907330bec3df80db29df14e44962a8766502e757849aa2` |
| Validator stdout SHA-256 | `b1c2a147fb74725565ef72a111f1bd72549637564956cdf7d69a60f0ef166410` |
| Toolchain | `go1.26.0 linux/amd64` |
| VCS build stamp | exact qualified head; `vcs.modified=false` |

The retained host reported an Intel Core i5-11400F, 12 logical CPUs,
33,512,759,296 bytes of memory, Linux 6.8.0-124-generic, and the artifact and
dataset paths on the same ext4 `/mnt/fast4tb` NVMe mount.

## Frozen configuration

- Two deterministic embedding-mixture corpora: 100,000 and 250,000 vectors,
  each with 1,000 held-out queries, 128 dimensions, cosine distance, and seeds
  4017 and 4016 respectively.
- Six freshly built M3 variants: graph-disjoint, graph-overlap at 0.20, and the
  stable-ID-hash disjoint control for each corpus; 16 partitions across four
  three-node Raft groups.
- Source-vector caps were 100,000/250,000 and expanded router-membership caps
  were 120,000/300,000. M3 exact-visit caps were 400M/900M; graph/router scalar
  caps were 20B/20B and 50B/50B.
- Every M8 repeat retained the full p1/p2/p4/p8/p16 ladder at `ef_search=128`,
  router-candidate budget 256, top-k 10, concurrency 1, and warmup 0. The
  selected matched-recall comparison is p2 versus exhaustive p16.
- M8 truth-visit caps were 600M/1.5B; peak RSS and persistent-asset caps were
  4 GiB and 2 GiB. Each corpus ran three serialized repeats.
- Graph variants bind KaHIP Python SHA-256
  `7d51cd6b48b521277f5caa4610a82126e315fa2be4df069823a8b1eeb5bd4a86`
  and adapter SHA-256
  `ae4ca8f5f26bd510a507a0f4ba50adaf1e5514ee9e20340cb9d494aba8f54825`.

## Anchored inputs

| Corpus | Fixture checksum | Manifest SHA-256 | Truth identity | Truth artifact SHA-256 | Truth semantic SHA-256 |
| --- | --- | --- | --- | --- | --- |
| 100k | `ecc2224f386932e580e4956f2cfa852140d3134625971c3511bc0d5feddf9b95` | `2548153084bd8a1c4dad91cd26033c870ab6e8fc9a354326f910e5d876e8919f` | `accdb76c693e2da99333b9327efc0e3d83ba630b25a8ba2b6820f5a6f6e38937` | `0e9bce9465c9e1fa70c7833364e88c332bc831cfc52c628c90085e1c3068763c` | `6e17b00a04ad86ad4b13507e6afc1ae38d323280b3a0aa8405bce88b222fa1bc` |
| 250k | `d0c7c82ba868853aae9a4280161003d72714ad1701d41ed3169c2fa94d470d69` | `14194cca83e94d776baf78897e423ba505d51b342cc189845e6b271945502025` | `f1fab20b88cd3dcdd6e95a284400983230b1432b36bd4d73e321e251159795ab` | `5a518c1cb8182edc685ab692dc17a6974655572f426a4b97c10482fd1643f04e` | `89b84125e518f33cc30bc1e4e9defcc0639378d7108fb180f56ec2dc91d6f254` |

The copied fixture manifests and original staged-input digest list are under
`anchors/`. The truth-cache payloads are bound by the artifact and semantic
digests but omitted from this compact manifest.

## Results

All six M3 commands and all six serialized M8 matrix commands returned zero.
Every matrix retained three reports, three transcript-v5 files, and 21 profile
artifacts in the full local bundle. The matrices included here retain the full
row-level recall attribution, coordinator ID/score parity, routing and local
HNSW work, resource ledger, exact commands, and profile/transcript digests.

| Corpus | p2 QPS min / median / max | p16 QPS min / median / max | p2 p95 ms min / median / max | p16 p95 ms min / median / max |
| --- | --- | --- | --- | --- |
| 100k | 55.2480 / 57.3518 / 58.2321 | 32.0320 / 36.0603 / 41.1363 | 24.715 / 27.181 / 27.261 | 35.663 / 47.663 / 55.574 |
| 250k | 56.6496 / 56.6941 / 56.7598 | 33.2721 / 39.4950 / 40.4955 | 25.779 / 25.884 / 26.533 | 37.339 / 38.818 / 52.980 |

M3 resource evidence:

| Corpus / variant | Wall time | Max RSS KiB |
| --- | ---: | ---: |
| 100k graph-disjoint | 6:08.68 | 1,043,660 |
| 100k graph-overlap-020 | 6:39.51 | 928,252 |
| 100k stable-ID-hash | 5:47.82 | 1,028,452 |
| 250k graph-disjoint | 17:51.74 | 2,004,012 |
| 250k graph-overlap-020 | 18:40.90 | 2,166,580 |
| 250k stable-ID-hash | 16:00.90 | 2,256,112 |

M8 resource evidence:

| Corpus / repeat | Wall time | Max RSS KiB | Matrix SHA-256 |
| --- | ---: | ---: | --- |
| 100k / 1 | 19:34.58 | 459,856 | `d2ef780100408ab9dac6bf1060f340a853464d6d05799d7a94295bbe4e3cd9b6` |
| 100k / 2 | 19:09.95 | 460,104 | `c49caee655f66f1a15c12622dfa1180d68bac5a89df5689f929aca0d196ca1cb` |
| 100k / 3 | 19:20.31 | 463,624 | `8f3b8a2d5cae41d9ebb22f2c626251c1dffd7b249da558b2af1dc839f78fe438` |
| 250k / 1 | 36:57.01 | 1,116,196 | `4211104c2db45465fd61549c366d755cf2b8220359d8ef2ddf5a8b375fbfbed8` |
| 250k / 2 | 36:56.30 | 1,051,164 | `80a103722d4dc1153ef935b2ca5b66590b195fc28181f58dccf982e5196e4347` |
| 250k / 3 | 36:15.94 | 1,042,892 | `96c8796f9a72d9c0dc4a653e2207d1b690ce2377573f526382bac863749dc21d` |

The single validator invocation returned status `qualified`, exit status 0,
wall time 1:44:10, maximum RSS 925,012 KiB, and empty stderr. The v2 campaign
index binds each matrix hash and a post-child publication timestamp; all six
execution/publication intervals were globally non-overlapping.

## Reproduction and review

The frozen commands and caps are defined in
[`TreeDB/docs/spec/artifacts/vector-partition-qualification-4027-plan.json`](../../../spec/artifacts/vector-partition-qualification-4027-plan.json)
and the evidence contract in
[`TreeDB/docs/spec/vector-partition-v1-qualification.md`](../../../spec/vector-partition-v1-qualification.md).
With the full self-contained bundle restored at one root, validate with the
retained binary and root-relative index:

```text
<campaign-root>/bin/treedb_vector_partition_bench validate-qualification \
  -index <campaign-root>/campaign.json
```

Reviewers can validate this compact manifest with:

```text
sha256sum -c SHA256SUMS
jq -e . campaign.json validator/stdout.json matrices/*/repeat-*/*.json \
  m3-descriptors/*/*/*.json anchors/*-fixture-manifest.json
```

## Limitations

- This is one Linux host and two deterministic synthetic embedding-mixture
  corpora, not a licensed external corpus or a multi-host generalization.
- Matrix disposition remains `local_gate_pass_multi_host_still_deferred`; the
  aggregate `qualified` result is scoped to the frozen #4027 contract.
- This compact directory is reviewable and hash-bound but is not independently
  validator-replayable without the omitted DBs, reports, transcripts, profiles,
  truth caches, retained executable, source clone, and logs.
- Exact commands intentionally retain the original absolute campaign paths.
  The schema-v2 index itself uses root-relative matrix paths.
