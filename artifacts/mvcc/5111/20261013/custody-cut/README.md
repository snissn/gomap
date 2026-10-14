# #5111 original-owner and authority-cut checkpoint

Production snapshot: `d73eac2f4bad0c54cde37403da00c9ddb10e5c94` (tree `1d1dbedc122447e266d9d9faa09a5b3b08dc0083`), based on `54893f48cb2a7a195b1b89198a39c87a9a1919c4`. This artifact-only descendant changes no production, test or harness file. The receipt is the immutable pre-publication checkpoint; its `no_push=true` records that moment, not the later publication status.

Scope and exact limits are in `docs/agent-execution/mvcc-4871-20261013/ordinary-primary-custody.md`. This is partial implementation evidence, not PR #5127 maturity, native acceptance, platform qualification or a performance pass. All original gates remain open until completed. Every raw log in the frozen receipt is retained, including failed intermediate runs, the broader timeout and baseline/candidate leaf-race failure.

The source-bindings in custody-cut-freeze.json are checked against the production commit. The receipt SHA256 is `5369ed0f19f82e58a828e641b2ff424f63705c25784d1a1ef4de975e36a2b694`; patch SHA256 `f5041e46bc82844bb404141864c686f8be40d4e1e636f5b33eb818a8634468f1`.

Final focused test arguments, passed as literal argv with Go1.26.4, GOWORK=off and GOMAXPROCS=2:

```python
["go", "test", "./TreeDB/db", "./TreeDB/zipper", "./TreeDB", "-run", "TestRootPublicationBuildGroupPrimaryV6|TestPrimaryConstructionV6|TestCOWPublicModeled", "-count=1", "-timeout=120s", "-v"]
```

Repeat with `-race` for the corresponding race packet. Full rootpublication/primaryarena normal command is recorded in the receipt. Original-constructor instrumentation requires the immutable original operands from `a468b6be387f03ea5393311196f82af91df4f972` and absolute-path rebinding described in the scope document; its instrumented runs establish original-object custody, not uninstrumented timing.

Production command-WAL PRIMARY selection, genuine composite successor-basis proof, caller/value/leaf/all19 lifetime reconciliation, Windows namespace correctness, complete fault/physical/cost/CI/review/final-base gates remain required. The unavailable8f20 packet may overlap current work; reconcile actual bytes if recovered, and borrow no missing evidence.
