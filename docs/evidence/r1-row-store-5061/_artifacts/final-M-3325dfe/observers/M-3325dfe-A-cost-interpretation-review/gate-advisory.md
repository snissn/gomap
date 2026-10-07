# Independent A cost advisory amendment

Original comparison: `a7c55ffc991c8630b31dcf7fcfb08e9c63c867f635615325978dfad62f343fde`. Original source and packet bindings remain in the preceding handoff and extract.

The typed checkpoint median rises from **1,110,120 to 3,829,760 B/call**, a **2,719,640 B increase (3.45×)**; allocations rise **5,849 to 6,534 (+685)**. Both checkpoint timing groups remain **INCONCLUSIVE**. Baseline already contains a 3,935,272 B sample, exceeding the candidate maximum 3,886,368 B. This observed sample overlap cannot identify causality or justify dismissing the median increase. Cumulative end-of-cell maintenance counters lack phase-start deltas and cannot attribute the additional allocations.

Independent read-only extraction of all five BSON setup samples confirms **1,264 to 3,512 B/call (+2,248 B)**, with median allocations **12 to 11** and timing **INCONCLUSIVE**. Baseline samples are 960/3568/1264/1056/3568 B; candidate samples are 3512/1000/3584/3336/3584 B. These are setup costs, not steady returned-row costs. Attribution remains unresolved.

The evidence demonstrates neither a new stable causal regression nor zero/harmless/noise allocation cost. All 20 stable old-path throughput guard groups pass; allocation and timing findings retain their original scope. Root, acting as acceptance coordinator, explicitly accepts the observed maintenance cost within this finite profile, with **causal attribution UNRESOLVED**. That disposition is separate from this independent advisory and is not a waiver of a demonstrated stable material regression. No universal maintenance/capacity improvement follows; D qualification remains mandatory.

No remeasurement, Go execution, source edit, original receipt/packet rewrite, or external action occurred. Review role released.
