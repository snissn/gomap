# Draft issue: recover graph/router locality at the V1 quarter-probe budget

## Problem

#4015's bounded 100k attribution fully realizes graph-overlap `.20`, but four
of 16 probes reaches only `.7201` recall@10 versus the `.90` target. Exact
partition-union parity passes. EF 256 reaches `.7289` at four probes while
exact representative routing is `.7291`; all-partition recall is `.9998`.
Thus this is graph/router primary-partition locality, not an EF-only issue.

Stable-ID-hash reaches `.2578` at four probes, so graph placement helps but is
not sufficient. This draft is a blocker input; it does not enable V1.

## Goal and evidence

Reach recall@10 >= `.90` at <=4/16 probes on declared qualification corpora
while preserving FP32 exact-union parity, balance, overlap <=1.35x and bounded
resources. Retain graph/disjoint, graph/overlap `.20`, stable-hash, and
all-partition controls; report exact routing separately from local HNSW; retain
EF64/128/256 and three repeats with median/spread. The required 1M row must
use explicit work/time/resource accounting or remain explicitly deferred.

## Non-goals

Do not relax `.90`, substitute 100k for 1M, or represent local-HNSW EF gains as
a routing fix. Close only with a retained V1 pass or an explicit negative
successor disposition.
