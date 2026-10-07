# Independent public-input cost identity v3 static review

**CLEAN** at `a79091f4ee3d7f265f8c330c60b2eb01139d658001f46de80cbc096a13d8a79f` against reviewed v2 `df71ce652147f3a55522e6c00f2a168f57266b66eb454060ce90eb792108c341`. Inverse one-literal removal yields exact original bytes and AST.

The only delta adds `actual_main_commit` to the explicit selected-source identity aliases, matching the original root A/C cost-record schema. Identity remains mandatory (`observed` must be nonempty), and every present recognized top-level/nested commit field must equal exact M. Conflicting aliases, nulls, missing identities and wrong commits therefore remain rejected. Duplicate-key/nonfinite rejection and original ledger-tracked hash-checked record reads are unchanged. No record reserialization or acceptance-field exemption is introduced.

Executor/template and all other builder logic, approval flags, compiler/packet/binary/receipt pins remain unchanged. This check supplies source applicability only; root approval and complete public ledger/replay gates remain separate. No helper execution, Go, tests, capture, remote/external action or activation occurred. Role released.
