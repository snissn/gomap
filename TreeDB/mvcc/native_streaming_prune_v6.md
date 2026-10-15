# Native streaming selection

The native cursor retains one original capture and immutable admission Request
while it scans history in physical order. A verified value anchor at or below
the discard floor makes each older verified record eligible for the current batch immediately. Its
key, envelope, pointer resolution/CRC and floor checks precede preparation.
Unrelated later records are not prerequisites for that deletion.

The private `integrityDone` scalar describes integrity prerequisites for the
current selected batch. It does not certify the whole namespace. It stays
true through publication/ACK and resets after the original checked transaction
receipt is detached. The same cursor then advances its stationary retained
projection rather than restarting the namespace scan.

A tombstone anchor is deferred until all its older records have been deleted.
The cursor retains the next logical-key boundary (or EOF), deletes the anchor,
and resumes at that boundary. Value anchors remain. A later malformed record
returns its real error without undoing prior ACKs or reporting global completion.
`Complete` means the captured namespace scan has reached its actual end and all
selected transactions have finished; one successful deletion is not completion.

The test-only unbounded diagnostic still synchronously drives the genuine
native producer, fence, Consume and checked Finish. Its work counters are
incomplete. Streaming selection improves when the first deletion becomes ready;
it does not establish Q32, a bounded publication window, production readiness,
or complete native-prune qualification. The bounded publication route continues
to refuse its unsupported final stage honestly.

`BatchSize` sets the retained vector capacity (default 256). The original cursor
admits the actual vector backing and each copied key/inline payload before
allocation. Work refusal preserves the same scan and buffered selections.
Publication rechecks every selected winner under one fresh canonical cut,
observes the same immutable Request for every key, and applies all deletes to
one original native constructor/zipper/fence/Consume/Finish. ACK counts the
actual finished vector. The stationary last selected row remains the original
constructor's physical selection; earlier entries are not substituted for it.

EOF flushes the actual remainder. Pending older history is published before a
tombstone anchor is queued, even when that requires a partial batch. A later
malformed record reports its error and preserves earlier completed batches;
verified but unpublished entries receive no ACK. A conflicting current winner
refuses the entire pending vector before physical deletion. Checked completion
detaches constructor aliases before phased vector clearing and payload refund.
Capacity admission failures are returned, rather than silently clamping the
requested batch size. Batching amortizes the synchronous diagnostic publisher;
it does not make its final publication stage bounded or establish Q32.
