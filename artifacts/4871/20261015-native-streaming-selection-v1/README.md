# Native streaming selection: public-path RED/GREEN

Source a465bdbd5fe1158dd130ab45871fcaacbf8ced60, parent7df5c827.
Exact four owned paths checkpointed and pushed; protected original77f14 and
foreign two files preserved. Root verified all4345 final source rows.

Before, actual public deletion/lifecycle checks passed, but the new first-ACK
activation regression failed: Visited1027 and Complete=true. After, Visited2,
Complete=false, per-selection integrity=true, ACK1 and36of37 DATA children retained.
The same public captured projection and immutable admission Request continue
through actual physical deletion, old-cut preservation, intervening durable
writes, checked Close and reopen. Focused continuation passes normal and race:
value anchors ACK3/Visited5, late malformed ACK2/Visited3 and truthful sticky error,
tombstone boundary ACK3/Visited4, tombstone EOF ACK3/Visited3. Anchor deletion
follows all older versions. Complete means actual captured scan completion.

Single public-call samples: before5110350ns/2406448B/3426alloc;
after925571ns/2107744B/1057alloc. Fixture/ordinary writes are outside the timer.
There are2276 versus53 intervening writes, so accumulated successor state differs.
These are descriptive functional samples, not a causal/statistical speedup or
total-throughput qualification. Phase16 publication is691117ns,74.67% of the new
public time,855920B/972alloc. Whole-call entry-phase buckets are not exclusive
inner-phase CPU attribution; process-wide MemStats is approximate.

RED/GREEN full source-before/source-after censuses are equal per run. Exact final
four source blobs match first-ACK GREEN. Final normal/race full4345 source censuses
equal the freeze census. Continuationv1 failure is retained: tombstone test queried
the forbidden discard-floor timestamp; correction changes the test to timestamp4.
No production workaround was made for that failed test.

The only publishing route remains the explicit unbounded diagnostic. Original
bounded phase16 still refuses. BatchSize currently yields one original producer
and publication per selected deletion. Batching, bounded preparation/current cut/
seal/physical Finish, original33/80/4096 qualification, Q32 and production cost
remain OPEN. This packet does not pass native or final graph acceptance.
Next structural priority is SAME constructor bounded publication and batching,
with focused public activation and immediate cost before broad qualification.
Parent4871 INCOMPLETE; M5 NO-GO. Other owners and original final gates preserved.
