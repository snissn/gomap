# Retained Docker 29 created-container inspect

These two JSON fixtures are exact bytes from the consumed, unsuccessful
`959cc5c491` reference correctness bootstrap, not a fabricated running service:

* `docker29-created.inspect.json`: raw Docker inspect stdout from action
  `support/mongodb/docker-0005.out`, immediately after create and before start.
* `docker29-bootstrap.manifest.json`: the accepted bootstrap manifest.

`provenance.json` binds their original hashes and records retained failure,
parent output and manual cleanup receipts. The original failure and raw evidence
remain at the recorded external task path. The created container never started;
manual exact-owned removal was verified separately. This fixture proves neither
MongoDB readiness nor any successful public lifecycle or product qualification.

The actual Engine 29.1.3 response omits `HostConfig.Init`. Locally cached Docker
SDK v25.0.6 and v28.5.2 declare `Init *bool` with JSON `omitempty`. The v28 source
comment says null uses daemon settings, and daemon/oci_linux.go adds an init when
the pointer is true or the pointer is nil and daemon configuration enables init.
The local evidence paths/file hashes and excerpts are retained in provenance.
Exact Engine 29 daemon source was not available locally, so these adjacent SDK
sources are not presented as an exact Engine 29 implementation audit.

Accordingly, omitted and null mean unspecified, not a verified disabled init.
Explicit false means disabled. Explicit true and non-boolean values are rejected.
Actual live PID1/ELF/namespace observations are still required independently; an
unexpected init wrapper cannot qualify through the optional configuration field.
Every other exact ownership/configuration check also remains required before
normal or emergency cleanup. Stopped-container cleanup needs no signal or wait.

Tests use the exact actual created inspect for checked identity and stopped
emergency removal with known/recovered full IDs. Running/terminal transitions
and all Docker commands in tests are explicitly synthetic mocks; they exercise
the same exact-owned SIGKILL/wait/terminal/remove path without contacting Docker.
