# Retained materialization component review

**ACCEPT the exact frozen five-file component. No findings. Native ACK remains0.**

Packet: `native-retained-materialization-20261015`

- Receipt SHA256: `8f067bae51b322db35aae626d821e5e5871037757bd966aeeeb6b9b0a16542e9`
- Manifest SHA256: `ca66d665b291b1f83c4fbaaf85d28fe3e9320114544fffa45791b677ccb6ac27`
- All30 manifest entries verified; both original files match immutable parent `353a88fb160febce951dd4caa45ac079c261fca4`. Full five source hashes are in review.json.

Ordinary count/encoding/emission drain the same retained cores. The prior encoding implementations are removed. Retained slot loops, fixed-depth walks and phased sink writes preserve progress; oversized backing growth explicitly refuses. Emission binds its original operands and keeps the first error without replay.

Each candidate page retains independent storage because the writer may retain its view. Caller-owned encode-Into buffers remain supported; controls alone are reused. Generic sinks keep isolated copies. No new pool, arena or second allocator appears.

Existing evidence independently binds prior-format reference bodies: reference-to-candidate4427-input censuses differ only at format.go, and all four emitted page hashes match. Five focused controls PASS in each run. Candidate freelist/public censuses are identical and unchanged within each run. The actual ./TreeDB public promotion reader/cut control PASSes. Decoder bodies are unchanged.

No Go execution, native source edits or Git/GitHub writes by reviewer. This accepts a source component, not the unfinished retained MaterializeCandidate constructor/reservation/header closure, native bounded public ACK, general Q32 completion or performance qualification. No architecture finding requires stopping the ordinary public performance guard.
