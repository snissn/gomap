# Native Wire v1 Fixtures

These fixtures pin the initial R0/R2 codec bytes for the internal native-wire
package. They are intentionally small and grow alongside command schemas,
negative conformance tests, and deterministic-entry coverage.

Deterministic entry fixtures use the v1 envelope:

```text
"TDC1" entry_version command_id command_version command_flags section_count
  section_id section_len section_payload ...
```

Envelope fields after the magic (`entry_version`, `command_id`,
`command_version`, `command_flags`, `section_count`, `section_id`, and
`section_len`) are minimal unsigned base-128 varints. Section payload bytes use
the encoding owned by each section schema.

Collection metadata V7 reuses the canonical catalog-create payload inside
`uvarint(7)`. It changes no command/section IDs, create command version or TDC1
envelope fields. Existing `create_collection_entry.hex` remains the legacy
payload fixture and must not be regenerated for V7. The separate
`TreeDB/nativewire/text_metadata_v7_test.go` control
`TestTextMetadataV7CanonicalCodecAndDeterministicCreate` checks exact new
payload bytes against the maintained catalog codec and re-encoding, avoiding a
second text serializer or a replacement of the legacy golden. Public lifecycle
and hostile-input cases in that file check the independent semantic contract.
