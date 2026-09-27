# Retained pack physical resource observations (#4775)

`treedb_vector_partition_bench physical-resources` emits a separate JSON receipt
with contract `untimed_retained_pack_physical_resources_not_qualification_v1`.
The existing `serving-resources` CPU/allocation v1 contract is unchanged.

Pass `-out <fresh-absolute-json> -source-checkout <clean-collector-checkout>
-head-sha <collector-commit> -executable-sha256 <collector-binary-sha256> --`
followed by the independently frozen `replay-m8-report` arguments. The collector
strictly replays the old parent first, retaining its source/executable identity
separately from the new collector's identity. It reopens the pinned assets with
the existing attribution harness: every physical pack for the per-pack runtime,
or exactly one validated domain anchor for each domain graph. It records its
own host, Go version, OS, architecture and page size. This is a new untimed
observation, not telemetry recovered from the parent's timed process.

For each retained searcher the receipt records:

- actual accounted mapped extents, including alignment (not RSS or resident pages);
- heap fallback payload bytes (not total Go heap);
- conservative prepared-view metadata and stable-ID accounting estimates,
  separately from payload bytes;
- logical manager handles (not OS file descriptors), including the pack root;
- required, opened and validated section chunks, excluding the root. Validation
  opens all these chunks; these counts do not describe actual query touches;
- the existing search-preflight scratch bound for every parent EF/top-k
  coordinate. Inventory totals sum one hypothetical search per retained
  searcher. They are not selected-fanout or concurrency peaks, nor measured
  scratch allocation/peak memory.

Observation occurs while all retained searchers are open and idle. There is no
query timing, CPU campaign, native topology, router accounting, or claim about
whole-process resources. Summed fields retain their individual meanings; mapped
extents and metadata estimates must not be combined into an RSS claim.

After closing the searchers, independent observers read their captured private
managers, requiring zero active handles/mapped/fallback bytes, balanced
acquire/release counters, and zero manager errors. A detached prepared pointer
alone cannot satisfy release verification. This verifies owned handle release,
not reclamation of Go metadata. No complete receipt is emitted after failure or
cancellation; a partial file or failed collector exit is never accepted.

The bounded strict reader `m8ReadPhysicalResourcesV1` requires an independently
frozen collector header, strictly replayed parent, and validated asset manifest.
It checks every serving identity exactly once, all scratch coordinates, all
per-pack sums (with overflow rejection), release completion, and the complete
marker. Unknown fields, truncation and trailing data fail closed. The reader
checks consistency; independently pinned command, executable, source and
receipt bytes plus observed exit status establish origin. Do not derive the
trusted expected header solely from the untrusted receipt.

Before applying observations to earlier retained assets, freeze and review the
collector and pack-reader implementation identities. The reporting extension
adds an offline diagnostic method; it does not change Status, preflight, search,
pack encoding/reading, or Close behavior. Neither these observations nor prior
CPU receipts alone confer performance qualification.
