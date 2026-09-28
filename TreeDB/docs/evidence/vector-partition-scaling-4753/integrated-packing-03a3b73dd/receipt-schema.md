# External receipt adapter scope

The unchanged landed strict CPU and physical resource readers remain the measurement validators. The external fReduce authenticator gains exactly one receipt kind: `in-process-comparer`.

Its existing receipt fields have these meanings:

- `Command`: the actual six-element `compare-m8-scaling -plan PATH -plan-sha256 SHA` invocation, including the frozen same-runtime producer executable.
- `Artifact`: hash and path of the original comparer `result.json`, not a projected replay stdout.
- `ExitStatus`: hash and path of the actual comparer process `exit-status.txt`, not an invented replay exit.
- Parent report `Pins`: the same independently frozen replay report, fixture, command, executable, variant and truth pins used by the landed strict replay.

The narrow helper authenticates the plan bytes against the actual command, disallows runtime overrides, requires a single unambiguous matching report argument list, and checks the verbatim receipt text for that report in the original result. The existing authenticator checks the executable and observed successful process exit before this helper; the existing resource consumer also validates the full parent report against its pins. The compile self-tests cover valid binding, duplicate/ambiguous parent, runtime override, altered/missing receipt, changed plan hash, unknown fields and truncation, as well as the existing shared receipt checks.

No derived command, replay exit, child-process telemetry, inferred query cost, or duplicate strict replay is produced. Comparer success authenticates its in-process replay even if the later performance flags reject. It is not a qualification verdict.

The pre-timing geometry overlay calls only the existing read-only opener, retained descriptor binder and exact logical-membership digest. It checks source/fixture/graph coordinates, D16/P16 versus D16/P64, planned targets, zero realized overlap and exact domain-union equality before timing. The later landed comparer repeats the union and serving-coordinate checks using the authenticated full reports.

The physical reader is the reviewed M topology projection helper with only the allowed arm IDs widened to one/multi block1. It authenticates the original parent and raw manifest before deriving the serving-view manifest from original group order; wrong raw identity, group order and unrelated header changes remain negative cases. It does not reinterpret old-process physical telemetry.
