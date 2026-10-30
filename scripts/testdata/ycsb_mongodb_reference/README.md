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

## MongoDB 8.2.7 actual failed readiness identity

`mongodb827-public-identity.extract.json` retains selected reply fields from
both actual public observation sequences in the consumed `833b9b2` attempt.
The full raw packet and close/owner/manifest paths and hashes are recorded in
the extract. It omits unrelated buildInfo/serverStatus fields but preserves the
actual requests, typed selected replies, connection IDs and checked results.
It is an explicitly labeled extract from failed readiness, not a complete or
qualifying lifecycle packet. No workload, explicit fsync or reopen occurred.

`mongodb827-git-origin.json` retains primary upstream public commit metadata:
public mirror commit9fbea533 declares GitOrigin-RevId8f678526, exactly the
reported buildInfo.gitVersion in that pinned actual image/ELF. It does not
claim a separately fetched origin tree. `mongodb827-provenance.json` binds both
fixture hashes. Pinned serverStatus/prettyHostName/default-port source excerpts
and full connector responses remain beside the original task's terminal audit.
Tests cover the actual default-port bare hostname and origin ID, preserve the
distinct public source identity, and reject substitutions without Docker or a
new service invocation. Original failed receipts remain unchanged.

## Actual UTC close logs from failed final validation

`mongodb827-close-log.extract.json` selects the unchanged actual close/completion
entries (IDs4795901 and8423404) from all four lifetimes of the consumed `0fb48d4`
attempt. Every actual date ends in `+00:00`. Original complete-log paths/hashes,
manifest, failure and terminal audit hashes are recorded. The attempt completed
its bounded public operations and owned cleanup but failed final validation;
these entries are not a complete or qualifying packet.

The extract also binds pinned upstream JSONFormatter and DateStringBuffer source
responses retained beside the original packet. Tests accept zero-offset and Z
syntax with exact nanoseconds, and reject malformed/unknown/nonzero offsets.
Synthetic complete-packet controls change matching log/raw-Docker copies and
hashes together; every lifetime still enforces signal→storage close→mongod
completion→actual exit chronology, including 1ns violations. No test invokes a
container or retroactively qualifies any failed attempt.
