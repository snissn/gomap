# C1 original receipt correction: root source lease

C1 is accepted in root-native-registry-api-core-review01-adoption.json. Native registry source01 stays rejected. All compiler processes joined; no test bodies ran. This source-only successor preserves source01 and every outside production/test migration byte.

Root owns only TreeDB/internal/retainedalloc/owner_work.go and new owner_original_reset_test.go for the C1 correction. Add ResetCompletedOriginal on the SAME surviving parent receipt, under its existing mutex: only completed original receipts reset; unused, bound, paused, failed and native work cannot be reset. Never copy or reset an initialized mutex. Preserve existing bounded reset semantics; share only field clearing as appropriate. Add discriminating owner/lease controls for exact repeated delta, incomplete/failure custody, and refusal to consume bounded work. Pinned gofmt only; no runtime from this selection.

The existing native_prerequisite worker will receive a separate db/lifecycle-only lease after concrete original-generation/header lifetime gating is selected. No overlap, no other source writer, no Git/index/GitHub changes, and no builds/tests until writers release and whole successor freezes. Remaining graph acceptance and unchanged public controls persist.
