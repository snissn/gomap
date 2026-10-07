# Linux publication portability preparation

The frozen helpers are usable on Linux with explicit path arguments. This is local read-only preparation; no Linux copy was independently inspected, no stage was created, and no evidence was qualified. Current candidate/landing and final A/C/D remain coordinator gates.

Use `/home/mikers/gomap-r1-mac-evidence-preservation-20261006` as the preservation source. The stager has no baked Mac root; the support assemblers do. Override `--source-base` on both. Their adjacent `assembly-plan.json` files must stay beside the copied helpers at their original bytes. Python 3.9+ is needed (`Path.is_relative_to` and `ast.unparse`); avoid Python `-O` for certified comparison.

Create a NEW selection after actual A/C/D qualification: start from the original13 records, replace each `root` prefix `/private/tmp/gomap-r1-execution-20261005/` with the actual Linux preservation prefix, and append the three actual records to reach16. Preserve every other historical field, packet byte, validation argv, source pin, rejection and destination. This changes location, not evidence. Do not edit the old selection. Check every original root is underneath that exact prefix, refuse duplicate/overlapping roots, and choose an output outside all inputs. Accepted new records need the independent observed validation log as a top-level samebyte file in a NEW input mirror if absent originally; producer validation alone does not supply independent acceptance. Mutation-sweep needs `mode.txt == r1-mutation-sweep`, not an invented A `summary.json`.

Future command templates (do not run before qualification/approval; all placeholders require actual frozen values):

```sh
PRES=/home/mikers/gomap-r1-mac-evidence-preservation-20261006
STAGE=/home/mikers/ROOT_SELECTED_NEW_STAGE
python3 "$PRES/prepare-evidence-publication-final.py" --captures /home/mikers/ROOT_SELECTED_NEW_16_RECORD_SELECTION.json --out "$STAGE"
python3 "$PRES/5061-support-assembly-complete-preparation/assemble-support-complete.py" --source-base "$PRES" --stage "$STAGE"
python3 "$PRES/supplemental-10-11-assembly-preparation/assemble-additional-support.py" --source-base "$PRES" --stage "$STAGE" --expected-helper-sha256 APPROVED_ADDITIONAL_HELPER_SHA --expected-core-sha256 INDEPENDENT_SELECTED_CORE_LEDGER_SHA
python3 "$PRES/verify-publication-replay.py" --stage "$STAGE" --out /home/mikers/ROOT_SELECTED_NEW_REPLAY
```

Use the approved complete nine-group assembler **instead of** the original six-group assembler. Both exclusively create `supplemental-support`; running both on one stage fails. Nine-group inputs are569 text files/32,958,914B; groups10/11 independently use `support-additional`,215 frozen/adoption files/26,312,080B. These figures describe frozen support, not acceptance. The additional helper requires its independently pinned SHA and the selected stage's independently frozen core `SHA256SUMS` digest. All support source modes must survive the copy. Both assemblers preserve the core ledger; additional10/11 also preserves any preexisting `PUBLICATION_SHA256SUMS`.

The original restorer checks/copies only files listed by core `SHA256SUMS` and restores indexed original binaries. It does **not** verify or copy the supplemental support ledgers/files or a final `PUBLICATION_SHA256SUMS`. Root must separately freeze/check the final publication ledger and each support ledger, preserve support in the public/downloaded text tree, and execute frozen semantic validators separately. Restore success explicitly leaves semantic replay pending.

Certified A comparison must receive actual relocated flags:

```sh
python3 "$PRES/final-A-semantic-equivalence-preparation/compare-certified.py" --certificate /home/mikers/ACTUAL_APPROVED_CERTIFICATE.json --expected-certificate-sha256 INDEPENDENT_APPROVED_CERTIFICATE_SHA --repo /home/mikers/ACTUAL_LANDED_GIT_REPO --before "$PRES/captures/r1-baseline-landed-0216e2e/packet.json" --after /home/mikers/ACTUAL_FINAL_A/packet.json --receipts /home/mikers/ACTUAL_FINAL_A_RECEIPTS --accepted-freeze /home/mikers/ACTUAL_ACCEPTED_FINAL_A_FREEZE.json --original-compare "$PRES/compare_r1_packets.py"
```

The baseline packet must retain adjacent `source.json`, `source-after.json`, `frozen-source-accepted.json`, `capture-environment.json`, `collection_workload_bench`, `validation.txt`, `buildinfo.txt`, and `cc-version.txt`. The selected packet likewise needs adjacent original executable/source/build sidecars; selected observation/receipt files stay together in the supplied receipt directory. Local inspection confirms baseline packet/freeze/environment/source/binary exist; exact hashes are in handoff.json. Baseline binarySHA898457b63c4ba9a3897150b4c9cacf09222830f87cf3616dfe73f479e7596812. Verify these on Linux rather than reconstructing them. Certificate uses actual raw byte hashes and independently approved selected source/runtime/harness, not paths invented from packets.

Historical config/receipt absolute paths are intentional observed strings. The comparator checks recorded argv against recorded config paths, but opens evidence through its supplied relocated path flags. Leave those observed strings unchanged. Environment must still derive from actual recorded before/build/after observations and match baseline host/device/compiler/Go environment; location portability does not waive comparability. The Git repo must contain the actual baseline and selected objects for `ls-tree`, `show` and exact reviewed diffs; the copied private root alone is insufficient. A current blank/preparation certificate is not approval.

All13 local selected roots and all three historical accepted comparator binaries/logs exist. Some rejected diagnostic roots intentionally have no original binary; preserve the missing-replay limitation rather than filling it with a later executable. No new16-record selection was observed at the execution-root top level. Handoff pins every inspected helper/plan/selection. Eight Python helper AST parses passed; no helper runtime/staging/semantic test was run. Frozen originals were read only.
