# Test-only stale-preparation fixture correction

Original focused green1 remains exit1: the public concurrent4096 test and other controls passed, while this test failed its original typed==nil assertion. No product success is inferred from the partial run.

The empty-overlay explanation is unsupported: native_engine_handoff.go:287 invokes cloneNativeTypedSuffix; native_typed_state.go:214-244 returns nil when current==captured boundary. The full-prefix typed==nil assertion is retained.

Source-plausible cause, not yet runtime-proven: the original fixture filled ReservedBytes exactly to the75% pressure watermark using a post-Close stats sample. Original off-worker cleanup can refund previously queued owners between that sample and the gated fresh observation. Correct production behavior is to skip maintenance once the fresh pressure predicate is false. The original log did not record that fresh observation, so this artifact does not declare the cause proved.

Correction uses the existing held-pressure control's genuine87.5% external reservation target, retaining product budgets/watermark/admission unchanged. It reobserves immediately before the contender is released and requires at least1/16 of the original budget above the75% watermark. Existing external and maintenance owners remain live through the assertion. This headroom is a fixture choice, not a bound on unknown garbage or a product allowance.

The assertion is strengthened with original captured basis token and logical sequence, exact before/after typed delta/row/delete counts, document generation, physical primary root and column-manifest root progression, matching active/recovery manifest identity, physical AppliedCommandLSN coverage, unchanged logical sequence/LSN/data and exactly-once later public Update body. It continues to require pending typed==nil; return-nil alone cannot pass.

Only TreeDB/collections/native_producer_gate_test.go changes relative to frozen candidate1. All other nine-path candidate1 artifacts and all cold files remain immutable. Root owns formatting, runtime and interpretation; no Go/gofmt/build/tests were executed by this worker.

Proposed root selector: TestNativeProducerGateRevalidatesStalePreparation, followed by its unchanged held-pressure peer and the original focused batch if justified. No production correction selected.
