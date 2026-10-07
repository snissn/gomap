# Pager-owned leaf-generation manifest format

Options.OwnedLeafManifests, available through both db and treedb, opts a **new
database** into this pre-alpha format with IndexOuterLeavesInValueLog enabled.
Rebuild into a fresh directory to change an existing standalone database.
The option defaults to false; default rollout requires the complete R1 gates.
In-place activation, feature removal and mixed physical layouts are refused.

Required feature owned_leaf_manifest_v1 and physical root-record version 3
provide independent refusal. Both bootstrap meta slots contain version 3.
Frozen797 refuses creation, first publication, repeated publication and older
fallback even with format.json absent/ignored. A current reader refuses
physical/config disagreement instead of falling back to standalone filenames.

## Ownership, encoding and publication

Immutability inherits TreeDB's exclusive index-writer and LOCK contract: the
engine never mutates pages retained by an immutable COW root. It is not kernel
protection against external writable FDs/mmap. Integrity checks validate visited
content; an unsupported external write to an unvisited unrelated index page
has no immediate global-detection guarantee. Legacy standalone stores keep
their whole-inventory, unrelated-corruption, rebind, pin, quarantine and sync
checks.

The exact candidate system root owns canonical JSON under reserved prefix
\x00treedb/owned-leaf-manifest/. The 64-byte header holds magic TDLMOWN followed
by byte 1, byte length, chunk count, revision, SHA-256 digest, and eight zero
reserved bytes. Ordinal keys use eight hexadecimal digits. Chunks are inline,
at most 2048 bytes, with an exact final remainder. Maximum canonical size is
64 KiB, maximum generations 128 and total file IDs 4096. Paths have depth 32.
Dimensions are admitted before allocation/load. The complete reserved key
interval rejects missing, wrong-ordinal, oversized, unknown or trailing keys.

This is intrinsic index content, never a ResourceOuterLeafManifest FD token.
Existing root records, both selectable slots, held roots, queued candidates,
recovery handoff, index generation and allocator pins retain it. Real external
leaf/value-log dependencies remain in the same root-bound resource closure.
No second history registry exists. PrepareLeafGenerationManifestStableClosure
refuses owned mode; CheckpointOwnedLeafManifest durably publishes a revision.
Online/offline vacuum copy both independent objects and update only the desired
replacement root. File shrink remains a separate vacuum result.

## Bounded reclamation and evidence

PruneOwnedLeafManifestStep resumes the existing allocator cursor. The cursor
is scheduling state and never authority; omission/reopen can repeat work.
Each destructive step freshly fences the exact root/epoch/index, held FD
identity, publication and snapshot admission, and opaque reuse capability.

Whole LeafGenerationGC retains one exact recoverable-root capture and validates
each distinct intrinsic object once for that pinned basis. Supported publication
or relocation invalidates the certificate and forces recapture. Digest or
metadata counters alone never certify mutable external aliases.

A step examines at most 64 entries, promotes at most 16 pages, visits at most
512 trie nodes and mutates one fixed-depth chunk path. Page/byte and whole-path
credits precede traversal/mutation; intrinsic lookup/range validation has finite
credits before reads. Metrics include actual reads/bytes, object validations,
physical identity checks and all cumulative setup/GC pruning. Prepared callers
receive a conservative 2176-page output allowance before command-WAL append.
Existing total-output and freelist caps must admit it; no cap is enlarged and
no conservative allowance is preallocated.

Owned metrics report internal retired/free/promoted pages and index high-water;
legacy unlinked-file counts/bytes remain zero. Roughly 40 metadata pages per
revision can accrue with a held root in the 512 guardrail. Setup, COW paths and
promotion costs remain visible. Drained equal-revision groups must demonstrate
actual page reuse and high-water/debt plateau. The whole-GC capture parallels
frozen standalone controls without a held root; held512 is a separate guardrail.
Freeze committed source, runtime inputs, fixture, binary and toolchain before
qualifying matched controls. Passing a helper fixture is not gate acceptance.
