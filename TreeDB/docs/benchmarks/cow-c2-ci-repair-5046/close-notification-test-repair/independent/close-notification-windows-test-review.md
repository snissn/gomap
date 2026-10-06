# Frozen Close notification test review

**ACCEPT** the frozen single-file test repair and its scoped local normal/race/safe evidence. Actual repaired Windows runtime and final-head hosted gates remain pending. No source correction is required by this review.

The base is `61fbd36655a9a8641f1cacee013c8f2bf2dc1553`; the reviewed working-copy test SHA-256 is `d15253d599b953a09baa79ef182ddf5bfd8814960040beed8243e3e6e6ccd548`. This is an uncommitted frozen delta, not a new commit. Git status and complete diff identify only `TreeDB/cow_close_notification_test.go`; the index has no changes. Before-source literal, actual Git diff, before/after source bindings and original failed log match. Production/runtime/module/authority and cost fixture bytes are unchanged.

The notification entry signal now occurs immediately before Stats/Get. Completion records both nil Stats and ErrClosed after the reads. The callback then waits for an explicit release, while the test checks Close has not returned. The original five-second read watchdog starts at observed callback entry. A separate single thirty-second timer starts before Close launch and spans both entry and storage teardown; expiration is classified by phase rather than asserted to be a callback deadlock. This retains a finite whole-Close deadline.

Real pressure admission and the returned wrapped final-checkpoint ErrCOWCapacity assertion are preserved. Pressure remains until actual completed Close on the passing path, then its release must leave retained-cache TotalBytes and ExternalLeases zero. Failure cleanup releases pressure and the callback gate, joins the started Close with a bounded thirty-second watchdog, and removes the manually owned directory only after the finished signal. Join failure marks the test failed and explicitly preserves the directory; it does not mask failed cleanup or race Windows open handles. The separate async Close-hook test is unchanged.

The nonblocking finished check is an observation at that point, not a universal proof against every future asynchronous callback implementation. Current public reportError still invokes NotifyError directly and synchronously; that unchanged source ordering supports this fixture's contract.

I parsed actual local normal/race/safe logs and receipts: each stage exits0 with18 test/subcase pass events and one package pass, count3 for each of two parent tests and four profile subcases. No fail, skip or race report; stderr is empty. These checks executed on the writer's local runner, not Windows. Source and raw evidence hashes are bound in the accompanying JSON; no reviewer jobs were run.

The added test body and imports change benchmark test binary inputs. Historical974 costs keep their original exact identity and apply through unchanged executed production paths/fixtures; this is not binary equivalence or new exact-head timing. No automatic45-run recollection is necessary for this test-only delta, but final-head strict hosted performance and actual Windows/requiredCI/review remain separate, unwaived authorities. Preserve the old014 failure and bind the actual future committed descendant and publication manifest independently.

Exact supporting artifact SHA-256 bindings:

- `before-source.json`: `e7c545a75a94552cb28079576d19eec70b9289e1e27c73544b83a85b72b7879d`
- `after-source.json`: `711a5c3b0fb0fc8e281d76c769d5ecdd2317ff1beed93cc9fedccdaa841696dd`
- `before.go.txt`: `35ebac9f24cf009d570bcfd9f397c57b98d3c7276f8bf306fe9df7a2516bc252`
- `runtime.diff`: `dc4eebe1a5e3fa6484eb74e785ec15efa00ce8306445799fddb9049c28f90be3`
- `run-targeted.py`: `b1c0b0229bd1333844ba5a76dd2e1e3df24582f8c558563aafb8c2e39f754525`
- `execution-receipts.json`: `f67e715174cc6867a6dd4b36a999d75ad7918060cf9c36730aca3eace20a5ce3`
- `go-environment.json`: `570e98b02f271d04447529d7f7221b7ccf34e8a562e05935fc9dfeffde427fc4`
- `normal.json`: `2f63f01ce62ad7814e6aa950d2629d9e4b79554429ca380406ec7c637e5b3cf8`
- `normal.stderr`: `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
- `race.json`: `f07cee9c92ec51befd9cad41a97ecc1a50ebe4e544d3e4da1018c3a6e40b81c5`
- `race.stderr`: `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
- `safe.json`: `792bb18ba9628442c079db1c7b3463fecb36a818f583e6de4fcb832e11c37483`
- `safe.stderr`: `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
- `original-windows-failure-events.json`: `5588313102446e6a592c076e4e2da101eb12f7c38b4b22384120a3770a4c756c`
- `original-windows-failure.log`: `27131c64bae66d140c8ad89237a4389f43433a756beb29d9317b099f7a8b1583`

Final literal18-file packet and exact no-extra inventory verified; packet-manifest.json SHA-256 `9ad95f1e40f5448b3292cb6730b8b9a30e4101ebda21036f199bd5ed12793dc8`. Local execution was Darwin/arm64 Go1.26.0, not the hosted Windows toolchain.
