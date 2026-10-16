# Selected ledger cursor refusal: frozen-source diagnosis

The command PASS is **not selected-ledger success**. This is a test-observer admission defect plus a test false-positive; no production cursor or retained-owner defect is shown by this failure. No source edit, Go command, runtime, retry or Git operation was performed for this diagnosis.

## Proven cause and skipped assertion

`mvcc/native_selected_ledger_test.go:98–102` starts a fresh Q32/1MiB work object and calls `Reserve(1, 2*sizeof(PruneCursor{}))`. The exact frozen runtime log reports cursorRaw=796032, so the requested indivisible unit is **1 record / 1,592,064 bytes**, exceeding **1,048,576** by **543,488**. `internal/iterator/ordinal.go:80–85` refuses without consuming records/bytes. The helper logs `cursor observer refusal` and returns before its 43 scalar accessor calls, every P row, and its final completeness Fatal at242–243. The comparison-admission log/return at144–148 is the sibling false-positive. The parent actual33 fixture does not inspect a returned completion result; execution continues through basis validation, newer writer, cancellation, old reader and reopen, hence its PASS.

This was an artificial whole-container read/write charge: the helper does not copy or scan the cursor's embedded 64KiB buffers, codec frames, prepared object, or projection. `sizeof` itself does not read them. Reducing actual observation to exactly named fields is not changing production allocation admission; the cursor remains originally charged and its production limits/latch unchanged. Do not split this fictitious full copy into two accounting turns or double the byte limit.

## Minimal coherent repair proposal (test source only)

Own `mvcc/native_selected_ledger_test.go`, plus directly matching pure refusal tests if needed. Keep every production primitive, storage owner, fixture, guard, limit and selected-role assertion unchanged.

1. Make both top-level early exits fail the expected observation: nil cursor, cursor-prefix refusal and comparison-admission refusal must Fatal (or return an explicit incomplete result which this expected-complete caller unconditionally Fatals on). Do not retain log-and-success returns. Existing `selectedSetConstructorRowsV6` early returns already propagate incomplete rows to the final Fatal; keep that behavior. A declined row addition must likewise leave Complete false and reach the final Fatal. Pure negative tests may deliberately assert incomplete, independently of this expected-positive actual33 call.
2. Replace the whole-cursor unit with **preadmitted original-field access**. For each accessor invocation obtain its original parent operand only after a fixed prefix reserve, using the SAME work object passed to that accessor. A pointer field read/local pointer result or address-of embedded capture needs **1 record / `2*sizeof(pointer)` =16 bytes on this pinned linux/amd64 image**. A retainedBytes uint64 read/local scalar needs **1/16**. This charges the actual named operand and local result, not the unrelated containing buffers. No new snapshot/root/resource/owner is acquired; locals must stay within this synchronous test helper and never escape.
3. Price each repeated prefix again; do not reserve once then silently reread `c.preparation`, `c.retention` or derive `&c.capture` in every later call. Alternatively load a fixed local operand bundle once after admitting its actual source/result fields and use only those locals, but choose and price one arrangement explicitly. The per-call arrangement below is simplest to audit against existing call sites and needs no new retained representation.
4. Keep each existing accessor's original nested Reserve calls/locks/readiness and the final independent completeness/overflow predicates. The added prefix is consumed from that accessor's Q32/1MiB turn; if the composition does not fit, report failure before access. No internal replenishment or fallback. Preserve basis revalidation before/after the group and report non-atomic observations truthfully.

### Exact repeated-prefix inventory for the proposed per-call arrangement

| Original parent | Invocations | Added records / bytes |
|---|---:|---:|
| c.preparation: retained13 + Set5 + pair10 + index2 + roles1 + private1 + frame1 + transaction1 + manifest1 + tokenPair1 + token2 + namedBank2 |40|40 /640|
| &c.capture: selected enrollment ledger, base capture, external lease |3|3 /48|
| c.retention: selected capture and external lease operands |2|2 /32|
| c.retainedBytes: P01 original policy scalar |1|1 /16|
| **Total across separately bounded observations** |46|**46 /736**|

The two capture calls with an outer lease require both prefixes (**2/32**) in their same work before the original nested call. Other calls add **1/16**. Do not count these 46 records as one quantum, as native F work, or as a publication allowance. They are repeated test observation traffic. The existing P01 scalar Reserve(1,16) already covers the last row: retain it or move it into the prefix helper, never charge it zero or count it twice. Thus new prefix debit beyond that existing P01 reservation is45/720.

### Other fixed evaluator units audited

The comparison gate stays **21 records / 6300 bytes** for this frozen layout: doubled sizeof(retained[13]=832, sets[5]=360, indexes[2]=656, roles=14, private=496, capture=72, tokens[2]=64, transaction=96, manifest=48), plus1024. This source-derived amd64 layout arithmetic is not a new Go measurement; keep code based on actual unsafe.Sizeof and let its original check refuse if layout changes. It fits the unchanged quantum, and refusal must now fail the expected-complete test.

For the observed supported four wrappers/two independent backing allocations, `selectedSetConstructorRowsV6` is **22 records /8376 bytes**: initial1/4384, five role visits5/760, all ten pair visits10/3040 (including the absent preparation role), and six independently checked additions6/192. Its early-return rows remain incomplete. Each selected row-add unit remains1/32; checked overflow never converts an incomplete row to zero/complete. Do not combine all observations or sum overlapping physical/Owner policy domains.

Every original accessor still independently prices its actual parent/header/nested reads; its existing cost is added to the explicit prefix, not replaced. The exact earlier raw group4 retained row was3/152000, Set row/pair4/148864, original index11/216272, named bank11/142832, so their same-source augmented turns would be4/152016,5/148880,12/216288,12/142848 respectively. Unexecuted selected enrollment/private/token accessor readiness and row completeness are **not proven by those sibling measurements**. Preserve their source expressions and fail closed if unavailable; no invented P total is justified.

## Verification proposal

First pure normal/race checks for field-prefix admission: zero budget and one-byte/one-record-short refusal must read/emit no facts, retain unchanged output; exact1/16 pointer/scalar boundary succeeds without touching a real cursor's unrelated buffers. Verify the two-operand capture prefix requires2/32. Pure status/completeness tests must cover nil cursor, prefix refusal, comparison refusal, missing ready flag, arithmetic refusal/overflow and inner Set evaluator refusal; expected-positive parent cannot report successful completion for any of them. Keep existing checked arithmetic and Set combinations tests.

Then freeze/review exact source and have root separately select ONE unchanged actual33 promoted previsibility observation. Require all21 P rows plus both H01 roles, PComplete/namedComplete=true, no early incomplete log accepted, original SAME two-index identities and original later writer/cancel/oldreader/reopen controls. This remains observation-only, no native publication or larger allowance. Any new row/cost refusal or unsupported geometry is a first failure, not permission to adjust limits or silently retry.

The raw observation02 packet remains immutable. Current F367/882745 and private capacities are diagnostic; selected P/P_named_roots were never evaluated. Q32 RED/ACK0 remains unchanged.
