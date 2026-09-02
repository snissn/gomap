| Engine | Scenario | RPS |
|---|---|---:|
| hashdb | clients=16;pipeline=512;time=5s;val=128;resp3=1;replyoff=1 | 5871911.20 |
| treedb | clients=16;pipeline=512;time=5s;val=128;resp3=1;replyoff=1 | 1785111.55 |
| redis | clients=16;pipeline=512;time=5s;val=128;resp3=1;replyoff=1 | 2423633.25 |
| valkey | clients=16;pipeline=512;time=5s;val=128;resp3=1;replyoff=1 | 2445651.19 |
| hashdb | clients=16;pipeline=512;time=5s;val=1024;resp3=1;replyoff=1 | 1042359.02 |
| treedb | clients=16;pipeline=512;time=5s;val=1024;resp3=1;replyoff=1 | 561478.17 |
| redis | clients=16;pipeline=512;time=5s;val=1024;resp3=1;replyoff=1 | 1169408.07 |
| valkey | clients=16;pipeline=512;time=5s;val=1024;resp3=1;replyoff=1 | 1125074.31 |
