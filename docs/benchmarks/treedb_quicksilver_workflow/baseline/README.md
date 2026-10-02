# Original-product comparison bridge

The original `ef22ce55b85524de2707453f22c5f8cd6ba89eaa` product and the
maintenance control `1f0b09b8ffad0c3eaa1eae0146958f45e57d8c1c` lack the
negative-filter option and statistics. The baseline-only test replacements
reject filter-on and require those statistics to be absent. They preserve the
canonical fixture, queries, sampling, CRC checks, phases, placement and reopen
proofs, using the separate `quicksilver-baseline-workflow-v1` packet schema.

[The reviewed patch](baseline-full-freeze.patch) applies to an external copy of
the canonical H source at `7e1c51d41bcc0392fcb17dec12e46fdab42449bc`; it does
not apply to production. Its SHA256 is
`87b6f7dd33a3eae4d52bb06716c1b4263174e6b5dff16b6bbe82ce502f43c728`.
It contains the two Go test replacements, the two amended capture helpers,
`prepare_baseline.py`, and the runnable `check_full_freeze.py`. Reuse the
unchanged canonical `scripts/treedb_memory_budget_overlay.py` alongside them.
Apply it only after checking the canonical source hashes in the
[independent construction review](independent-construction-review.md).

Prepare each baseline in its own checkout with the exact original tracked
HEAD/tree. Add only the five named canonical test/capture files, preserving
their hashes in an external `baseline-source-manifest-v1` manifest. The
manifest separately pins the external replacements, capture/overlay helpers,
preparation helper and actual compiler. Record the manifest digest independently
before passing it to `prepare_baseline.py`. Preparation compiles without
running and refuses pilot mode, tracked edits, unallowed additions, ignored
source additions, symlinks and source/tool drift. Normal candidate preparation
continues to require a clean reviewed checkout.

[The Mac construction packet](construction.md) records the exact successful
preparation and negative checks. Its absolute paths and compiler are historical
construction evidence. It is not a Linux freeze or a retained result. The
maintenance control needs its own preparation. Before retained collection,
verify the actual landed H blobs against the canonical hashes, resolve the H/T
statistics adapter, and generate fresh host-specific manifests and freezes for
each product. Record the actual landed source proof separately from the
reviewed prototype identity. Changed canonical bytes require reassessment of
the affected replacements and validators.

Run the existing quicksilver capture/validation flow through the external
baseline helper with the external runtime, harness and freeze identities.
Preserve every failed packet. Pilot evidence cannot satisfy retained validation.
Issue #4895 still requires repeated Linux comparisons, complete value/miss and
owner checks, successful native compaction/GC, and old-generation pin evidence.
Canonical reads precede updates; concurrent update read tails are supplied by
the separate algorithm-work capture. This construction makes no performance,
cold-device, whole-RAM or Quicksilver-equivalence claim.
