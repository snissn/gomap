# Original cursor tail admission correction

Source-only continuation of root-native-finaldriver-source-selection.md
(c3b1e699386b05bf79acdb2e60c444d622ec506bbee6e95b5630a08f243c2d27).
The coordinator fully read the independent immutable-5cde cursor report
native-finaldriver-cursor-progress-bound.md, SHA
03553d5eddf80b4aeeac60ac05338cc529858444533e5947bb5c551c3e7a8884,
and its JSON 0d9cfe3943f492a479bc6cc7878f0eb428d4a1f52775398ffb23ff13892c5815.
The same original cursor has three fixed traffic boundaries requiring explicit
admission before they can be included in the native finite-work guarantee.

1. Admit the actual last buffered key copy before beginBatchFlushV6 effects.
   The namespace-exhausted current projection key may be empty and cannot price
   this copy. The phase10 caller must not clear its current record or lose its
   retry state before a new refusal point. Preserve the original phase and
   original buffer/projection identity on refusal.
2. Admit the actual concrete retention resize at phase20 before truncation,
   refund or scalar reset. Use the existing same-lease work-bearing operation,
   not a new owner, accounting domain, grant or compensating ordinary refund.
   The same final unit also includes the original anchor-key copy when
   flushBeforeAnchor is true. The codec interface's other already-admitted
   synchronous refunds are not automatically replaced by a new lifetime model.
3. Admit the original concrete final lease-close traffic before closeStep
   detaches the containing lease. Public Close and coordinator Close retain
   their existing alias-clear-before-close order and exactly one physical
   close. An existing coordinator close admission may conservatively duplicate
   a bound, but must not create a second lease close or replenish public work.

The native worker owns these changes and focused refusal/retry controls within
the current coherent source packet. Every new refusal must retain original
custody and converge under a sufficient unchanged grant. No Go/runtime or
publication allowance is granted by this selection. Original Q32/1MiB/33/80/4096
and ACK0 remain historical evidence, not a successful native publication.
Final complete-source freeze, independent review and focused normal/race checks
remain required before a separately authorized actual driver.
