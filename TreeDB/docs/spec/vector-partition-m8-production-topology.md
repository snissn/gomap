# Vector partition M8 production topology foundation

Status: experimental foundation for #3917; not M8 closeout evidence.

## Boundary

`VectorPartitionShardSearchTCPDispatcherV1` and
`VectorPartitionShardSearchTCPServerV1` carry the existing M5 request and
response envelopes across a bounded, length-framed TCP connection. This is a
serialized service boundary: the M6 coordinator does not call an owner-local
function or consult an in-process dispatcher registry.

The adapter preserves M5's typed error code, owner group, and leader redirect
hint. A transport, framing, or remote-service failure returns an error with an
empty response. The M6 coordinator retains responsibility for bounded fanout,
one permitted `not_leader` retry, cancellation, and its no-partial-result rule.

## M8 CI topology to build next

The first complete CI topology must use two distinct owner groups with three
real `HashicorpRaftProvider` nodes per group. Each group exposes one TCP M5
service endpoint, obtains its own read-index/apply proof, and owns a disjoint
subset of at least four logical vector partitions. The coordinator reaches
those endpoints only through the TCP dispatcher.

The smoke artifact must be labeled `production_multi_group` only after that
topology has run. Existing M0 deterministic simulation and M6 local-service
artifacts remain respectively `simulation` and `local_service`; neither is
production evidence.

## Deliberately deferred

This foundation does not yet claim the 10k two-group test, M7 lifecycle
rebuild/failover matrix, 1M corpus, matched-recall curve, profiling campaign,
or M8 north-star gates. It only supplies the transport boundary those runs
require.
