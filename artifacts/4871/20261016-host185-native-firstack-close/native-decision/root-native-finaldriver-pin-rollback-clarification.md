# Identity pin admission: rollback scratch is not an eligibility predicate

This source-only clarification retains root-native-finaldriver-pin-admission-addendum.md and corrects the extra rollback-zero condition suggested in resource advice. It authorizes no execution, numeric grant or new owner lifetime.

In immutable source manifest 5cde43f24dcc86eaa8c2017bbc6cb0dc81a29e749d9420d27be2ab4f2e4e140d, retainedalloc/owner.go:114 sets each binding rollback to its previous admitted value before Resize. Lines119/122 read that field only to undo earlier bindings during that SAME atomic resizeLocked call. Successful completion does not zero it. The only other read in non-test retainedalloc source is owner_work.go:667, which sums it for observation. shrinkStepLocked does not read it.

The selected helper holds the SAME Owner.mu throughout capability inspection, pre-admission and nonfallible original removal. No resizeLocked call can be in flight under that lock. Therefore a nonzero rollback value is stale scratch, not evidence of a pending transaction, and rollback==0 must be removed as an eligibility predicate. Do not add normalization or a clearing traversal. The original concrete lease payload, current binding admission and owner state remain the proof operands; require the selected open/nonretired/nonfailed/no-enrollment geometry, current payload sufficient for the original shrink, checked all-binding capability and complete original work admissions before caller teardown. All other selected restrictions remain.

Focused controls should include a binding that grew successfully and consequently has nonzero rollback scratch; enough admitted work must consume that genuine last-idle pin exactly once, and refused work must leave custody unchanged.
