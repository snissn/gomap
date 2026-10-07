Prepared NEW public replay helper versions; every other original byte, logic branch, CLI flag and unapproved field is preserved. No helper execution, Go/build/test/benchmark, approval, measurement, remote write or new-file transfer occurred.

Exact diff scope:

- replay-public.py: only the hardcoded certified_A_compare SHA changes from 5bc6c7eb0c00b8f78860e31769ac4731f1d1aed2f91013e2a9b1ed7633065c75 to 3c4502ebb4620ac010b615ca10bbdd45a4448f66e2c7ed663d57c32413c1c2c2.
- build-draft-inputs.py: only cert_name changes to final-M-3325dfe/results/A/root-approved-A-semantics-and-provenance-certificate-v2.json (existing PREFIX preserved), plus TEMPLATE_SHA changes to the new exact template SHA.
- root-inputs.template.json: only certified_A_compare stage_relative becomes validation/compare-certified-v2.py and its SHA becomes 3c4502ebb4620ac010b615ca10bbdd45a4448f66e2c7ed663d57c32413c1c2c2.

Read-only SSH inspection verified all three original root-supplied hashes. Exact literal replacements preserve all other bytes. Static AST inspection proves executor/builder equality after reversing only those permitted string constants; JSON equality proves precisely the two template fields changed. All root approval, certificate approval and lane acceptance flags remain false. The reviewed adapter CLI has the same eight required options; neither it nor any replay helper was imported or executed.

New immutable pins:

- replay-public.py: 1bcdfa4ebc4489b2292caeb25a3f5ffd38a1adb347a0e455e8f4d736c4a07fcc (34425 bytes).
- build-draft-inputs.py: df71ce652147f3a55522e6c00f2a168f57266b66eb454060ce90eb792108c341 (22314 bytes).
- root-inputs.template.json: 7386b8a571eb6c0831f8004f5e6753bd0a07a2ba970b56d2840b3599369d0ac4 (15652 bytes).
- static-checks.json: 7c92a2385d326c167e8e33fabf15751613eb50214fb54c543cf23cd95f64f4d2.

Original byte observations, all three literal diffs and a full preparation inventory are retained beside this handoff. Root owns independent adoption, activation, actual public staging and approval; this preparation supplies no new numeric decision.
