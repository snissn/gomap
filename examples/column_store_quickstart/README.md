# TreeDB Activity Event Column Store

This command models a small feed-service activity stream. The service receives
events from web, iOS, and Android clients whenever someone posts, likes,
reposts, or follows. Each row records when the event happened, who acted, what
they did, which client sent it, and which feed item was touched.

The storage goal is deliberately mixed:

- Keep the full event available by id for product paths, debugging, and audit
  views.
- Keep the hot rollup dimensions scan-friendly for operational questions over a
  slice of the feed.

TreeDB supports that shape by storing each event as JSON while also writing a
few selected fields into physical column lanes.

## Run

Run from the repository root:

```sh
GOWORK=off go run ./examples/column_store_quickstart
```

Useful flags:

```sh
GOWORK=off go run ./examples/column_store_quickstart -rows 1000
GOWORK=off go run ./examples/column_store_quickstart -dir /tmp/gomap-column-demo -keep
```

## Event Shape

Rows model activity events:

```json
{
  "time_us": 1700000119002000,
  "action": "post.created",
  "actor": "did:plc:bruno",
  "client": "ios",
  "subject": "feed:storage-updates"
}
```

## Storage Layout

The collection stores the complete JSON row, then promotes the fields that are
useful for scans:

| Field | Storage | Motivation |
| --- | --- | --- |
| `time_us` | `int64` column, sort key | Event ordering and per-actor time spans. |
| `action` | Dictionary-capable string column | Group the feed slice by activity type. |
| `actor` | Dictionary-capable string column | Roll up activity by identity. |
| `client` | Retained JSON payload | Useful on primary reads, not scanned here. |
| `subject` | Retained JSON payload | Useful on primary reads, not scanned here. |

Primary reads reconstruct the full document from the retained payload plus the
column lanes. Column scans can answer rollup questions without reconstructing
every JSON row.

## Flow

The command performs the full lifecycle that a small application path would
exercise:

1. Generate activity-event JSON rows.
2. Create a durable TreeDB collection with column storage enabled.
3. Insert the batch and checkpoint the database.
4. Reopen the database to prove the persisted shape is readable.
5. Fetch one event by id.
6. Run two explicit physical column scans.

## Queries

| Query | Column Work | Question |
| --- | --- | --- |
| `events_by_action` | Group-count on `action`. | What kind of activity is dominating this feed slice? |
| `active_window_by_actor` | Group by `actor`; compute `max(time_us)-min(time_us)`. | How spread out is each actor's activity in the batch? |

## Output Excerpt

The terminal output starts by making the read path and scan path visible:

```text
Activity event column store
Scenario: a feed service receives activity events from web, iOS, and Android clients. Each row
records when something happened, who acted, what action they took, which client sent it, and
which feed item was touched.

Why collect it: the application still needs exact event reads by id for debugging and
user-facing workflows, while operators need fast rollups over a slice of the feed to see
activity mix and actor spread.

Storage shape: TreeDB stores the full event as JSON, then promotes time_us, action, and actor
into physical column lanes because those are the fields scanned by the rollups below. Client
and subject stay in the retained JSON payload.

╭──────────────────────────────────────────────────────────╮
│  Dataset                                                 │
│                                                          │
│  db:             /tmp/gomap-column-store-quickstart-...  │
│  collection:     activity_events                         │
│  rows ingested:  48                                      │
│  source:         feed activity events from app clients    │
│  row shape:      time_us, action, actor, client, subject  │
│  column lanes:   time_us, action, actor                  │
│  retained JSON:  client, subject                         │
╰──────────────────────────────────────────────────────────╯

Primary read after reopen
The row is still fetched by id as a complete JSON document; the columnized fields are stitched
back into the document.

╭──────────────────────────────────╮
│  Reconstructed row               │
│                                  │
│  id:       activity_0007         │
│  time_us:  1700000119002000      │
│  action:   post.created          │
│  actor:    did:plc:bruno         │
│  client:   ios                   │
│  subject:  feed:storage-updates  │
╰──────────────────────────────────╯

Column scans over the same rows
Both scans read the physical column lanes directly, so the rollups avoid reconstructing every
JSON document.
The plan is forced to serial_column_scan so the physical column path is explicit.

╭───────────────────────────────────────────────────────────────────────────────────────────────╮
│  Scan plan and counters                                                                       │
│                                                                                               │
│  query                   plan                rows  groups  bytes  MiB/s  JSON rows  manifest  │
│  ──────────────────────  ──────────────────  ────  ──────  ─────  ─────  ─────────  ────────  │
│  events_by_action        serial_column_scan  48    4       ...    ...    0          1         │
│  active_window_by_actor  serial_column_scan  48    9       ...    ...    0          1         │
╰───────────────────────────────────────────────────────────────────────────────────────────────╯
```

Headings and box labels are bold in a normal terminal. Set `NO_COLOR=1` to
disable ANSI styling.

For benchmark and profile evidence, use the column-store suite under
`./bin/unified-bench`.
