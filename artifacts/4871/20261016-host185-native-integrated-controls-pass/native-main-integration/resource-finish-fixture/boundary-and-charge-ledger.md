# Source-derived fixture boundaries and charge lifecycle

Only TestPrimaryFinishResourceRefusalAndFailureCustodyV6 changed. The source stage prices and exact byte expressions are in boundary-and-charge-ledger.json; they bind the same original descriptor, drop/table lookup, native queue and RemovalWork. All byte limits remain1MiB, and both final success turns remain32 records. Production F293/R145, containing layouts, work prices and caps are unchanged.

| Probe | Records used | Calls/effects | RootDrops/BanksFreed delta | Charge/custody |
|---|---:|---:|---:|---|
| Original3 | 3 | 0/0 | 0/0 | Original root/constructor/Set/charge preserved before lookup. |
| Added5 | 5 | 1/0 | 1/0 | Logical root handed to SAME queue; containing and bank charges retained. |
| Retained11, retry arm | 2 | 2/0 | 1/0 | Queue10 refuses before any physical effect. |
| Fresh32, retry first progress | 31 | 3/0 | 1/1 | Original bank/node scrub+refund complete; containing charge and SAME Set/constructor survive the next empty10 refusal. |
| Fresh32, retry final | 12 | 4/1 | 1/1 | Original empty queue observed, SAME Set released once, original terminal returned. |
| Fresh32, failure arm | 2 | 2/1 | 1/0 | Genuine first physical receipt retained, no queue teardown or containing refund; zero-work retry/Abort preserve it. |

Capture metadata owner Bytes before the constructor as baseline, and actual original construction metadataCharge after construction. This fixture has no enrolled bindings, promotion or other outputs. Once its bank and pager node are detached/refunded, Owner.Bytes must equal baseline+constructionCharge; callback return does not authorize subtracting the containing frame. The final returned original terminal has the same pointer, owner and exact charge; only existing completePrimaryTerminalsV6 after the Finish call returns changes Owner.Bytes back to baseline. Calling that helper twice must not replay root drop, bank free, Set effect or refund. The helper is original synthetic-fixture accounting, not evidence of genuine production native finite Finish.

Three probes separately retain their original custody assertions rather than increasing an allowance. No new API, successful native DB activation/publication authority, owner or registry was created. No Go command, compile or test body ran. Full source archive/one-function invariance is frozen separately for root review. Q32 RED/ACK0; no allowance.
