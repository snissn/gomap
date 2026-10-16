The first landed maintenance diagnostic stopped in its pure Python checks before any Go build or benchmark. Python 3.12 adds `FunctionDef.type_params` to `ast.dump`, so the unchanged collector failed hashes produced with Python 3.10 ([failed run](https://github.com/snissn/gomap/actions/runs/38830037922)).

Bind the three reviewed functions to their exact source bytes instead. Their source still matches the original reviewed collector; the fixed inputs, fourteen-call schedule, parser, limits and workflow are unchanged.

Validation: all 10 pure controls pass on Python 3.9.23 and 3.10.12; emulating Python 3.12's added AST field reproduces the exact hosted mismatch. `git diff --check` passes. Independent exact-head review and required CI are pending. No Go build, benchmark or diagnostic redispatch is part of this PR.

Related to #4871 and follow-up to #5137.
