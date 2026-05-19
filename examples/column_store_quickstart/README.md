# TreeDB Activity Event Column Store

Activity streams are a natural fit for a collection that needs both primary
reads and scan-friendly analytics. Each event is stored as JSON so the original
row can be read back by key, while a few high-value fields are also written into
physical column lanes.

The command writes a small activity feed into TreeDB, checkpoints it, reopens
the database, reads one event back by id, and runs two column scans over the
same data.

Run it from the repository root:

```sh
GOWORK=off go run ./examples/column_store_quickstart
```

Useful flags:

```sh
GOWORK=off go run ./examples/column_store_quickstart -rows 1000
GOWORK=off go run ./examples/column_store_quickstart -dir /tmp/gomap-column-demo -keep
```

## Data Shape

Rows model activity events such as posts, likes, reposts, and follows:

```json
{
  "time_us": 1700000119002000,
  "action": "post.created",
  "actor": "did:plc:bruno",
  "client": "ios",
  "subject": "feed:storage-updates"
}
```

The collection keeps three fields in the column store:

- `time_us`: ordered `int64` event time
- `action`: dictionary-capable string for event mix queries
- `actor`: dictionary-capable string for per-actor rollups

The remaining JSON fields stay in the retained payload. A primary read can
still reconstruct the full event from the retained payload and the column lanes.

## Queries

`events_by_action` answers the operational question, "What kind of activity is
dominating this slice of the feed?"

`active_window_by_actor` groups by actor and calculates the observed time span
between that actor's first and last event in the batch.

The output keeps the read path and scan path visible:

```text
Activity event column store
Write a short activity stream as JSON, keep the hot dimensions in column lanes, then reopen it
and scan those lanes for rollups.
+-- Dataset ----------------------------------------------------+
| db:             /tmp/gomap-column-store-quickstart-...        |
| collection:     activity_events                               |
| rows ingested:  48                                            |
| column lanes:   time_us, action, actor                        |
| retained JSON:  client, subject                               |
+---------------------------------------------------------------+

Primary read after reopen
The row is still fetched by id as a complete JSON document; the columnized fields are stitched
back into the document.
+-- Reconstructed row -----------+
| id:       activity_0007        |
| time_us:  1700000119002000     |
| action:   post.created         |
| actor:    did:plc:bruno        |
| client:   ios                  |
| subject:  feed:storage-updates |
+--------------------------------+

Column scans over the same rows
Both scans read the physical column lanes directly, so the rollups avoid reconstructing every
JSON document.
The plan is forced to serial_column_scan so the physical column path is explicit.
+-- Scan plan and counters -------------------------------------------------------------------+
| query                   plan                rows  groups  bytes  MiB/s  JSON rows  manifest |
| ----------------------  ------------------  ----  ------  -----  -----  ---------  -------- |
| events_by_action        serial_column_scan  48    4       ...    ...    0          1        |
| active_window_by_actor  serial_column_scan  48    9       ...    ...    0          1        |
+---------------------------------------------------------------------------------------------+

What activity is in this feed slice?
The first scan groups on the dictionary-coded action lane.
+-- Events by Action ---+
| action         events |
| -------------  ------ |
| graph.follow   8      |
| post.created   19     |
+-----------------------+
```

Headings and box labels are bold in a normal terminal. Set `NO_COLOR=1` to
disable ANSI styling.

For benchmark and profile evidence, use the column-store suite under
`./bin/unified-bench`.
