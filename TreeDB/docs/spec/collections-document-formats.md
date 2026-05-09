# Collections Document Formats

This document defines the current collection document formats.

TreeDB is pre-alpha. Collection document formats and public helper APIs may
change without compatibility guarantees.

## 1. JSON Format

The default collection document format is JSON.

- `CollectionOptions.DocumentFormat=""` and `"json"` both select JSON.
- The primary collection root stores caller-provided document bytes.
- Secondary index extraction parses JSON documents during insert, delete, and
  index backfill.

## 2. Template V1 Format

`CollectionOptions.DocumentFormat="template-v1"` selects the template-v1
binary document format.

Template-v1 is optimized for workloads with repeated object shapes. It removes
field names from each stored document and stores field names once per template.
Very high shape cardinality can reduce or erase the benefit; future format
selection may choose JSON or another encoding for those workloads.

### 2.1 Template map storage

Each template-v1 collection owns a collection-local TreeDB ordered root:

```text
<collection>/templates
```

This root is the template map. It is dictionary-like storage, but it is not a
process-local cache and it is not the global value-log `dictdb` side store. It
is a normal TreeDB root so template records, primary documents, index state, and
secondary-index postings can be published from the same collection batch and
read from one snapshot.

Template root entries are:

```text
key   = 0x00 "n"
value = uvarint next_numeric_template_id

key   = 0x01 || SHA-256(template_record_bytes)
value = uvarint numeric_template_id

key   = 0x02 || uvarint numeric_template_id
value = template record bytes
```

The template root currently uses the collection data-root storage policy. The
production-priority collection matrix keeps data roots in value-log outer-leaf
mode and varies secondary index roots between pager leaves and value-log outer
leaves.

### 2.2 Template record bytes

Template records start with `TD1T`:

```text
bytes  "TD1T"
uvarint field_count
repeat field_count:
  uvarint field_name_len
  bytes   field_name
```

Field names are sorted lexicographically and must be unique. Empty field names,
NUL bytes, and `.` are rejected. The `.` byte is reserved because collection
index paths use dot-separated segments.

The template hash is:

```text
SHA-256(template_record_bytes)
```

The collection template root maps this stable hash to a compact numeric
template ID. Stored documents use the numeric ID so repeated documents do not
carry a 32-byte hash in the hot path.

Nested objects get their own template records.

### 2.3 Stored document bytes

The primary collection root stores compact template-v1 document bytes beginning
with `TD1D`:

```text
bytes  "TD1D"
uvarint root_numeric_template_id
repeat fields in root template order:
  value
```

Values are encoded with a one-byte kind followed by kind-specific payload:

- null
- false
- true
- float64
- string
- object
- array

Object values store a nested numeric template ID followed by values in that
nested template's field order.

`Collection.Get` currently returns these stored `TD1D` bytes for template-v1
collections. It does not reconstruct caller JSON.

### 2.4 Insert envelope

Inserts may provide a self-contained input envelope beginning with `TD1I`:

```text
bytes  "TD1I"
uvarint template_count
repeat template_count:
  bytes  template_hash[32]
  uvarint template_record_len
  bytes  template_record
bytes insert_document
```

The insert document embedded after the records is hash-addressed:

```text
bytes "TD1H"
bytes root_template_hash[32]
repeat fields in root template order:
  value
```

The collection planner persists any template records from the envelope into the
`<collection>/templates` root, assigns or reuses numeric template IDs, rewrites
the hash-addressed insert document to compact `TD1D`, and stores only that
compact document in the primary root.

Scoped encoder inserts may also provide compact `TD1D` bytes directly after the
same encoder has learned numeric template IDs for the logical collection.
Ordinary `InsertBatch` rejects bare `TD1D` input because numeric template IDs
are collection-local; use `InsertBatchWithTemplateV1Encoder` for learned-ID
bytes.

`TemplateV1Encoder` is a stateful helper for repeated shapes. It emits template
records the first time a shape is seen and then emits hash-addressed `TD1H`
insert documents for the same shape. `Collection.InsertBatchWithTemplateV1Encoder`
teaches the encoder the numeric template IDs resolved by a successful insert, so
later `EncodeDocument` calls can emit compact `TD1D` bytes directly when all
templates in the document are already known. Learned numeric IDs are scoped to
one logical collection because template IDs are collection-local. Do not reuse
an encoder with learned IDs across collections; `InsertBatchWithTemplateV1Encoder`
rejects a learned encoder bound to a different collection. Call `Reset` before
switching collections or after a failed or abandoned publish attempt if the next
batch cannot rely on the earlier template records having been persisted.

### 2.5 Index extraction

Template-v1 secondary index extraction resolves numeric template IDs through
the same collection-local template root used for writes.

- Insert batches resolve templates from the batch-local records first and then
  from the persisted template root.
- Delete and index backfill resolve templates from the persisted template root.
- Nested paths such as `profile.city` are resolved by following nested object
  template IDs.
- Multikey indexes can index arrays of scalar values.

This keeps JSON parsing quarantined to JSON collections. Template-v1 collections
still pay value decoding and template lookups, but they do not pay per-document
JSON parse cost or repeated field-name storage for repeated shapes.

## 3. Benchmarking

The focused collections benchmark harness accepts:

```text
TREEDB_COLLECTION_DOCUMENT_FORMAT=json
TREEDB_COLLECTION_DOCUMENT_FORMAT=template-v1
```

Benchmark reports and matrix summaries include the document format label so
JSON and template-v1 throughput are not mixed.
