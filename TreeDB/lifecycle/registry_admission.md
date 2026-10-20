# Reader registry capacity and lifetime

Production index generations construct `NewReaderRegistryWithMetadata` using
their original metadata owner. The registry header (including all 16 fixed
shards) is admitted before allocation. The offline PRIMARY scope uses its
original PRIMARY metadata owner instead. Unowned diagnostic registries retain
`NewReaderRegistry` and use the same registration/error signatures.

`Register`, `RegisterWithHint`, and `RegisterPageRetention` return admission
errors. Callers must unwind the existing capture/publication preparation on
refusal; zero is never a successful growable registration. Ordinary
`AcquireSnapshot` preserves its pointer-only API and returns nil on capture
refusal. `RegisterFastWithHint` keeps its fixed-storage, zero-on-refusal API and
performs no growth or metadata admission.

When fixed sequence/purpose cohorts are occupied, fallback sequence and free
backings grow together. The original owner admits both complete replacement
backings while the originals remain charged. After copying and detaching the
old edges, their charge is returned. The retained free capacity always covers
every sequence slot, so `Unregister` and slot reuse allocate nothing. Released
slots keep their backing capacity charged until generation disposal.

Physical cleanup failure retains the same generation, registry, capacity and
accounting authority. Successful terminal disposal detaches registry backing
and its charge through `DetachMetadataCharge`; the original generation's
by-value completion returns the combined charge only after final containing
aliases and locks end. The detach operation requires existing generation
lifetime exclusion; it is not a concurrent registry shutdown API.

All-pin reuse/prune minima and reader-only asset minima are unchanged. This
capacity change adds no source limit, registry, governor, retry, or permission.
Growth can now refuse where the previous implementation allocated implicitly;
existing prepare rollback and postassignment error custody remain authoritative.

The source packet adds refusal/coexistence/reuse assertions, extends the
original generation refund and real Snapshot cleanup-failure controls, and
retains purpose/epoch race coverage. Runtime correctness and the bounded public
cost rebind must be established on the exact candidate before acceptance.
