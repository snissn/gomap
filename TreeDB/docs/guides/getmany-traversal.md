# GetMany callback pointer values

Grouped `GetManyView` pointer callbacks prefer the existing key-aware append
reader, then the key-aware view reader, then the generic append reader, then
the generic view reader. This matches iterator value routing and preserves
key-aware lookup when only a generic appender is available. The append route
can reuse the value log's existing compressed grouped-frame cache; CRC checks,
cache identity, admission limits and manager-wide retention budget remain in
force. Cache hits copy selected bytes into the callback destination, so this
route borrows neither cache slots nor mapped value storage.

One page-sized destination is acquired lazily per grouped tree batch when an
appender first reads a pointer. Inline-only batches and reader-only routes do
not acquire it. It is separate from the active leaf-page scratch and is reused
only after each callback returns. Returned backing is never saved in scratch;
values larger than a page use temporary output, leaving the pooled destination
at one page. Repeated oversized values can therefore allocate repeatedly.
Callbacks must still copy values they retain. Owned `GetMany` results and
`GetAppend` path counters keep their existing behavior.

The per-key leaf planning, small-batch fallback, callback indices, duplicate
and missing-key behavior, root pins and leaf leases retain their current
contracts. This change does not introduce shared internal-node traversal.

Focused route, lifetime, bounded scratch and actual CRC-verified compressed
frame cache checks use the existing tree fixtures:

```sh
GOWORK=off go test ./TreeDB/tree -run '^TestTreeGetManyPointerView' -count=1
```

Full-size paired latency, allocation and memory qualification must use the
current selected source and harness. Earlier allocation pilots on provisional
shared-planner branches are historical evidence only.
