# Native codec continuation seam

This is a provisional lower-source construction checkpoint for #4878, not the
#4871 production SAMERequest qualification. The original tree projection cursor
uses `COWLeafProjectionReader` for actual native leaf frames. Raw, Snappy and LZ4
continue to use their existing allocation-free paths. A configured ZSTD reader
must use the scoped `StepWithCodec` / `CancelWithCodec` entry points; ordinary
`Step` / `Cancel` reject an unbound codec work object.

The enclosing original admitted owner supplies immutable File/FileID, the whole
containing Input/Raw/TemplateScratch allocations, continuation controls, and the
exact dictionary or template generation owner and definition. Definition IDs are
not custody. `COWCodecRetention` delegates directly to that owner's existing
admission/release methods; it does not create an independent lease ledger.
Production calls reuse that owner's preborn current `OrdinalScanWork` object.
Library callbacks borrow it only for a concrete native call, and `DropWork`
follows every outcome. Cancellation clears all lower aliases before refunding
codec control/storage; the enclosing owner alone closes the persistent value-log
file or retires its resource.

The native ZSTD caller delegates to the original bounded library engine:
NewBoundedReader, Begin/BeginStream, InputRequest/FeedInput, Step, RawResult,
ResetStep/ResetStreamStep, and CloseStep. Cold dictionary SHA binding, original
binary dictionary parsing, block tables/history, input reads and CRC, emitted
copies, reset and cleanup consume the SAME work object before effects. A pending
stream input slot survives a refused FeedInput without rereading. Actual ReadAt
chunks are at most 4096 bytes. Finished physical frame aliases are detached
before caller input/raw backings are overwritten for another frame.

Leaf input/raw limits describe original leaf geometry (255 * 4096 = 1044480 raw
bytes) and do not restrict pointed values. `COWZSTDContinuation.BeginFile` accepts
the original producer payload range, an admitted 4096-byte input slot, and the
validated original header/directory CRC seed. Necessary original history and
BoundedSpans support pointed payloads exceeding 2 MiB. Emitted spans remain
uncommitted until whole input closure and original outer CRC validation succeed.
A complete decoded value is never allocated as a fallback.

`COWTemplateContinuation` suspends the original native anchor and mask definition
and payload grammar using scalar cursors. It checks the complete definition CRC,
all anchor/position bounds and mask tail bits before derived output effects.
TemplateScratch is a distinct, admitted page backing. No parsed-definition cache
or generation lookup is introduced, and the supplied immutable definition owner
remains retained until Cancel. It supports original template v1 anchors and v2/v3
masks, including full masks and definition-position overrides.

Focused controls exercise actual native writers and original decode oracles,
255-item full/compact ZSTD leaves, physical reread and retarget, cold dictionary
identity/refusal, no-work pauses, cancellation/custody, corrupt definitions and
masks, original tree forward/reverse revisions/tombstones, and >2 MiB streamed
pointed Spans plus outer CRC failure. The original allocation-free raw/Snappy/LZ4
control caught a per-call work escape; scoped binding fixes that RED without
weakening its assertions.

Still required from production glue: reuse the original admitted PruneCursor /
capture / owner path, bind actual registry definitions to these lower continuations,
phase physical Finish, and run the protected unchanged SAMERequest33/80/4096
harness as the first usable actual native ABI. Component controls do not establish
that gate or final performance, resource16/file17/two-index, or full packet claims.
The run-local module replacement is construction-only; public go.mod must await
the reviewed and merged fork version.
