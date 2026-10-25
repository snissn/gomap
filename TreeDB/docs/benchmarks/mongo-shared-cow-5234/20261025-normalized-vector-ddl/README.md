# Normalized native vector DDL repair

Repair source: `0ffdf97324dabc96a4bdea1e37ceb65db5813f1b`, after published evidence head
`2235461290dc60e150267ac8abe9ec3ae651480f`.

Exact-head PR review found that ordinary `CreateVectorIndex` with an omitted
name created `vector-index/` instead of the normalized field-derived domain.
The old-source [public regression](native-unnamed-ddl-red1.log) fails with
`normalized=false empty=true`. The repair retains the normalized definition
already returned by `addVectorIndexToCollectionMeta` and uses it directly for
the representation guard and domain. It removes the failed lookup by the original
empty name, without changing locking, admission or publication ownership.

The maintained public lifecycle now has eighteen cases: declared/named creation
and six new omitted-name cases across JSON/BSON and all three durability profiles.
They check the normalized domain and absence of an empty-name domain, insertion
and graph maintenance, build/search/save, checkpoint, repeated close/reopen and
held old graph state. [All eighteen pass](native-unnamed-ddl-green1.log) in8.250s;
[changed-risk race](native-unnamed-ddl-race1.log) passes in11.553s.
The [independent review](native-unnamed-ddl-review.md) accepts the exact repair.
The complete [collections owning package](native-unnamed-ddl-collections1.log)
passes in501.491s after race; [exact commands and statuses](native-unnamed-ddl-validation.json)
are retained.

## Performance evidence applicability

The original [106-process report](../20261025-d0407930e/README.md), binary hashes,
raw results, failures and accepted tradeoffs remain bound to measured runtime
`d0407930ef138915306e1a89c2c80522174c50ad`. They are **not measurements of the
repair source or its newly linked binary**. No archive or numerical result is
replaced. The preceding publication head223 was documentation/evidence only;
this later repair is a separate runtime change.

The protocol invalidates affected evidence. Independent source review accepts
applicability to the unaffected measured workloads: scalar primary/resource
updates never execute vector DDL; all three vector fixtures create the explicitly
named `embedding` index before their timing/allocation intervals. The returned
normalized definition is the same definition formerly found by that name, so
that setup produces the same domain. Timed Build/Save/Load/Search and physical
Close/reopen implementations and all seven landed harnesses are unchanged.
This is a bounded source applicability decision, not a claim that compiler layout
or the new binary is byte-identical, and does not extend old evidence to the
separately changed document-service/Hybrid consumer lane.

The required current-head CI and hosted review still gate merge. The preceding
223 raw-path/strict PRIMARY pass and separate retained M0 CV failure do not certify
the repair head. No threshold, durability or original failed result is waived.
[source-binding.json](source-binding.json) records immutable source/evidence identities.
