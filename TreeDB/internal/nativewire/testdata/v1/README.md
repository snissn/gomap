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
