# Finaldriver fixture repair01

Only `TreeDB/zipper/materialized_input_growth_work_v6_test.go` changes from the rejected 282a5adb source. The original stopped normal-zipper log/results remain untouched in `../finaldriver-source/controls`. All production source, price/formula/retention helpers, real diagnostic arms and prior proposals are unchanged.

The original fixture expected zero owner bytes while its genuine enrollment was still open. Its enrollment and binding own 192 bytes independently of MaterializedInputWork. Each constructor/entries/payload arm now captures Owner.Bytes and COWBudget ExternalBytes immediately after Enroll and before input construction; after original CloseStep and typed RemovePendingWork, both counters must equal that baseline. It then closes the SAME original enrollment and requires both counters zero; deferred idempotent Close remains. Both bounded cleanup loops now require an explicit completion flag. All growth refusal, same producer, original alias-detachment and fixed 32-record/1MiB cleanup assertions remain.

`freeze.json` and the full source archive/manifest bind the single-file successor. `source.delta.patch` and `changed-source.tar.gz` isolate the exact repair. No Go/compiler/gofmt/test bodies or actual native publication were run by this repair worker. No production or G/F/Finish/retention pricing changed.

`reused-controls.json` references the exact four prior passing normal calls (17 top-level/33 child tests), whose production and test inputs remain unchanged. The fresh ten-call proposal selects failed normal-zipper, previously unexecuted normal-db/freelist and all seven races, with a fresh absolute TMPDIR under this repair packet per command. These commands remain executed:false and require root selection. No repeated passing normal call, native finaldriver, benchmark, actual33 or numeric allowance is proposed here.

Both source and runtime leases are released at this freeze. Current Q32 remains RED/ACK0; historical phase26 proof is not relabeled current.
