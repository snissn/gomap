## Dict Micro-batch K Tradeoff (2026-01-11)

Command:
```
go run ./TreeDB/cmd/kv_dict_batch_bench -input tmp/treedb_kv_full.jsonl -dict tmp/dict-32k.zdict
```

Results (eval rows 200000..249999, cap 512B, dict=32KB):

| K  | batches | payload_ratio | total_ratio | decD1_ns/row | decD2_ns/row |
|----|---------|---------------|-------------|--------------|--------------|
| 1  | 50000   | 0.6545        | 0.6851      | 2263         | 2246         |
| 2  | 25000   | 0.4954        | 0.5184      | 2724         | 1337         |
| 3  | 16666   | 0.4386        | 0.4591      | 3357         | 1112         |
| 4  | 12500   | 0.4090        | 0.4281      | 3972         | 975          |
| 5  | 10000   | 0.3916        | 0.4100      | 4613         | 884          |
| 6  | 8333    | 0.3802        | 0.3981      | 5112         | 832          |
| 7  | 7142    | 0.3721        | 0.3896      | 5629         | 786          |
| 8  | 6250    | 0.3665        | 0.3837      | 5899         | 714          |
| 16 | 3125    | 0.3419        | 0.3582      | 7390         | 441          |
| 32 | 1562    | 0.3250        | 0.3408      | 13504        | 406          |
| 64 | 781     | 0.3166        | 0.3321      | 24143        | 369          |
| 128| 390     | 0.3105        | 0.3259      | 48111        | 355          |

Conclusion:
- Near-streaming total_ratio ~0.33 at K≈64–128.
- Best trade for bounded point-read decode is K≈3–8 (default expected in this range).
- Gold standard goal: dict micro-batching should approach ~0.33x while keeping worst-case decode bounded.
