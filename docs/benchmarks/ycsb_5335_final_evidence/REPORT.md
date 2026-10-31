# Twelve-arm comparison report

COMPLETE OFFLINE PROJECTION — merge/review acceptance remains separate

This is an offline projection of accepted maintained lifecycle evidence. All sample slots and rejected prior attempts are listed. Timing totals exclude client Close. Random runs establish observed state preservation; the separate deterministic fixture checks exact values. Scoped ACK/crash controls are separate evidence. No power-loss certification or quiet-host claim follows. Ratios below one remain negative performance observations; evidence completeness does not satisfy an unmet performance target.

Actual gateway source: `8bea490ad5a2d89a14f02f428b4283579a6dd864` / tree `8dc682ffb04281e63d57d3aeb485dcc41fc066dd`.

Continuation: two samples retain their prior packet identities; ten use the continuation packet. All 13 attempt slots include the rejected original G2, excluded from samples. Original STOPPED and all prior statuses remain in report.json.

| Class | Pair | Mongo load ops/s | GoMap load ops/s | Mongo run ops/s | GoMap run ops/s | GoMap/Mongo |
|---|---:|---:|---:|---:|---:|---:|
| relaxed | 1 | 39516.3 | 17262.4 | 38106.2 | 48493.6 | 1.272591 |
| relaxed | 2 | 38852.6 | 17457.9 | 42753.6 | 59245.2 | 1.385736 |
| relaxed | 3 | 40043.1 | 17491.6 | 43242.7 | 59339.8 | 1.372250 |
| durable-B | 1 | 40561.6 | 2173.5 | 21094.7 | 11666.9 | 0.553073 |
| durable-B | 2 | 39542.8 | 2185.0 | 19223.8 | 12290.8 | 0.639353 |
| durable-B | 3 | 40152.8 | 2205.7 | 19879.2 | 11766.6 | 0.591905 |

| Class | Mongo median ops/s (range) | GoMap median ops/s (range) | Median paired ratio |
|---|---:|---:|---:|
| relaxed | 42753.6 (38106.2–43242.7) | 59245.2 (48493.6–59339.8) | 1.372250 |
| durable-B | 19879.2 (19223.8–21094.7) | 11766.6 (11666.9–12290.8) | 0.591905 |

| Attempt | Operation | Count | Errors | Mean µs | p50 µs | p95 µs | p99 µs | p999 µs | Max µs |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| relaxed-pair1-mongodb | READ | 9502 | 0 | 406.0 | 364.0 | 723.0 | 1077.0 | 2289.0 | 3849.0 |
| relaxed-pair1-mongodb | UPDATE | 498 | 0 | 436.0 | 389.0 | 773.0 | 1133.0 | 2077.0 | 2077.0 |
| relaxed-pair1-gomap | READ | 9545 | 0 | 305.0 | 242.0 | 701.0 | 1086.0 | 1971.0 | 2957.0 |
| relaxed-pair1-gomap | UPDATE | 455 | 0 | 505.0 | 442.0 | 997.0 | 1224.0 | 1985.0 | 1985.0 |
| relaxed-pair2-gomap | READ | 9506 | 0 | 251.0 | 186.0 | 598.0 | 864.0 | 1132.0 | 1731.0 |
| relaxed-pair2-gomap | UPDATE | 494 | 0 | 450.0 | 399.0 | 875.0 | 1096.0 | 1574.0 | 1574.0 |
| relaxed-pair2-mongodb | READ | 9457 | 0 | 359.0 | 323.0 | 638.0 | 966.0 | 1704.0 | 2445.0 |
| relaxed-pair2-mongodb | UPDATE | 543 | 0 | 374.0 | 337.0 | 641.0 | 887.0 | 1290.0 | 1511.0 |
| relaxed-pair3-mongodb | READ | 9508 | 0 | 359.0 | 324.0 | 625.0 | 910.0 | 1738.0 | 3165.0 |
| relaxed-pair3-mongodb | UPDATE | 492 | 0 | 369.0 | 333.0 | 611.0 | 905.0 | 1259.0 | 1259.0 |
| relaxed-pair3-gomap | READ | 9484 | 0 | 250.0 | 187.0 | 578.0 | 825.0 | 1194.0 | 1679.0 |
| relaxed-pair3-gomap | UPDATE | 516 | 0 | 457.0 | 408.0 | 867.0 | 1040.0 | 1103.0 | 1108.0 |
| durable-B-pair1-mongodb | READ | 9487 | 0 | 198.0 | 173.0 | 356.0 | 562.0 | 1600.0 | 1920.0 |
| durable-B-pair1-mongodb | UPDATE | 513 | 0 | 9040.0 | 9935.0 | 11951.0 | 12247.0 | 12471.0 | 12487.0 |
| durable-B-pair1-gomap | READ | 9489 | 0 | 962.0 | 1021.0 | 1725.0 | 2245.0 | 2943.0 | 3385.0 |
| durable-B-pair1-gomap | UPDATE | 511 | 0 | 7703.0 | 7691.0 | 10895.0 | 13503.0 | 15271.0 | 16087.0 |
| durable-B-pair2-gomap | READ | 9516 | 0 | 944.0 | 1009.0 | 1688.0 | 2069.0 | 2531.0 | 3199.0 |
| durable-B-pair2-gomap | UPDATE | 484 | 0 | 7624.0 | 7615.0 | 10455.0 | 11511.0 | 12751.0 | 12751.0 |
| durable-B-pair2-mongodb | READ | 9524 | 0 | 197.0 | 169.0 | 378.0 | 605.0 | 1125.0 | 1533.0 |
| durable-B-pair2-mongodb | UPDATE | 476 | 0 | 11656.0 | 10871.0 | 23631.0 | 28223.0 | 29727.0 | 29727.0 |
| durable-B-pair3-mongodb | READ | 9489 | 0 | 187.0 | 165.0 | 340.0 | 582.0 | 1295.0 | 1435.0 |
| durable-B-pair3-mongodb | UPDATE | 511 | 0 | 9602.0 | 10303.0 | 12263.0 | 18943.0 | 21167.0 | 21743.0 |
| durable-B-pair3-gomap | READ | 9471 | 0 | 927.0 | 995.0 | 1695.0 | 2079.0 | 2869.0 | 3593.0 |
| durable-B-pair3-gomap | UPDATE | 529 | 0 | 7539.0 | 7439.0 | 10719.0 | 11927.0 | 12167.0 | 12767.0 |

| Attempt | Load s | Run s | Load DB Close s | Run DB Close s | Load workload Close s | Run workload Close s | Sentinel s | Supervisor s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| relaxed-pair1-mongodb | 2.836248311 | 0.262795022 | 0.000434425 | 0.000507828 | 0.000000203 | 0.000000179 | n/a | 45.608938314 |
| relaxed-pair1-gomap | 5.793488885 | 0.206375176 | 0.000289440 | 0.000624488 | 0.000000107 | 0.000000202 | n/a | 85.724303585 |
| relaxed-pair2-gomap | 5.728776053 | 0.169064304 | 0.000324666 | 0.000282320 | 0.000000099 | 0.000000115 | n/a | 84.946541262 |
| relaxed-pair2-mongodb | 2.885656980 | 0.234179121 | 0.000418223 | 0.000427100 | 0.000000197 | 0.000000220 | n/a | 46.007319120 |
| relaxed-pair3-mongodb | 2.806996500 | 0.231947368 | 0.000395422 | 0.000385739 | 0.000000200 | 0.000000136 | n/a | 45.953364256 |
| relaxed-pair3-gomap | 5.717915493 | 0.168857660 | 0.000292807 | 0.000267937 | 0.000000119 | 0.000000136 | n/a | 84.900495807 |
| durable-B-pair1-mongodb | 2.770929379 | 0.474635473 | 0.000540839 | 0.000462154 | 0.000000129 | 0.000000212 | 0.062123000 | 46.267001595 |
| durable-B-pair1-gomap | 46.016258898 | 0.857793547 | 0.000407023 | 0.000313483 | 0.000000131 | 0.000000108 | 0.149853000 | 127.240025786 |
| durable-B-pair2-gomap | 45.772830448 | 0.813893340 | 0.000353924 | 0.000336300 | 0.000000133 | 0.000000117 | 0.142878000 | 126.257425976 |
| durable-B-pair2-mongodb | 2.829815445 | 0.520575336 | 0.000415280 | 0.000448787 | 0.000000139 | 0.000000239 | 0.062903000 | 47.346999993 |
| durable-B-pair3-mongodb | 2.800205022 | 0.503281888 | 0.000404497 | 0.000425086 | 0.000000105 | 0.000000126 | 0.062537000 | 46.389961040 |
| durable-B-pair3-gomap | 45.344032669 | 0.850283120 | 0.000330259 | 0.000289302 | 0.000000100 | 0.000000117 | 0.238060000 | 126.813412247 |

| Attempt | Actual server build | Requested write concern | Profile | Peak RSS bytes | Max watchdog gap s | Late observations |
|---|---|---|---|---:|---:|---:|
| relaxed-pair1-mongodb | `{"architecture": "amd64", "docker_cli_sha256": "87bb910f051879cdf2a4a38e01748cbebeb27f3de962122dfdcbd0a4fbeea4ff", "exe_sha256": "d767fb752139fbc8e0e667d7e0c6fe7cdf690e0aa94a51502a32bad8c54a3672", "git_version": "8f6785264a643d2eeb5e4046f7876a23e40e1a09", "image_id": "sha256:690f1371c118a8389ecd6c82c9a7f0e37320732b1d56935a24b29dfca6933f54", "image_labels": {"org.opencontainers.image.version": "24.04"}, "kind": "mongodb_image_build", "os": "linux", "repo_digest": "mongo@sha256:7abfba0d07c9330373f8173981ea4d09cd8a82cdf0e86ccaf7008848d1d24f62", "runtime": {"cgroup_version": "2", "containerd": "2.2.1", "default_runtime": "runc", "driver": "overlay2", "engine": "29.1.3", "engine_commit": "29.1.3-0ubuntu3~22.04.2", "runc": "1.3.3-0ubuntu1~22.04.3"}, "schema_version": 1, "source_commit": "9fbea53313e85fbbf864a963ff17b0bd6dafa0be", "version": "8.2.7"}` | `{"w": 1, "journal_requested": null}` | mongodb_w1_journal_unspecified | 2143461376 | 0.15232194401323795 | 389 |
| relaxed-pair1-gomap | `{"exe_sha256": "35409b3e8a34e9af8cd578ac00ef72cb0d721abbcccfd9437eab67e050df4ee4", "expected_owner": "cow", "schema_version": 1, "source_commit": "8bea490ad5a2d89a14f02f428b4283579a6dd864", "source_dir": "/home/mikers/gomap-5335-landing-20261030/worktree", "source_patch_sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", "version": "candidate"}` | `{"w": 1, "journal_requested": null}` | command_wal_relaxed | 3300524032 | 0.15470655728131533 | 755 |
| relaxed-pair2-gomap | `{"exe_sha256": "35409b3e8a34e9af8cd578ac00ef72cb0d721abbcccfd9437eab67e050df4ee4", "expected_owner": "cow", "schema_version": 1, "source_commit": "8bea490ad5a2d89a14f02f428b4283579a6dd864", "source_dir": "/home/mikers/gomap-5335-landing-20261030/worktree", "source_patch_sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", "version": "candidate"}` | `{"w": 1, "journal_requested": null}` | command_wal_relaxed | 3187646464 | 0.1466993661597371 | 741 |
| relaxed-pair2-mongodb | `{"architecture": "amd64", "docker_cli_sha256": "87bb910f051879cdf2a4a38e01748cbebeb27f3de962122dfdcbd0a4fbeea4ff", "exe_sha256": "d767fb752139fbc8e0e667d7e0c6fe7cdf690e0aa94a51502a32bad8c54a3672", "git_version": "8f6785264a643d2eeb5e4046f7876a23e40e1a09", "image_id": "sha256:690f1371c118a8389ecd6c82c9a7f0e37320732b1d56935a24b29dfca6933f54", "image_labels": {"org.opencontainers.image.version": "24.04"}, "kind": "mongodb_image_build", "os": "linux", "repo_digest": "mongo@sha256:7abfba0d07c9330373f8173981ea4d09cd8a82cdf0e86ccaf7008848d1d24f62", "runtime": {"cgroup_version": "2", "containerd": "2.2.1", "default_runtime": "runc", "driver": "overlay2", "engine": "29.1.3", "engine_commit": "29.1.3-0ubuntu3~22.04.2", "runc": "1.3.3-0ubuntu1~22.04.3"}, "schema_version": 1, "source_commit": "9fbea53313e85fbbf864a963ff17b0bd6dafa0be", "version": "8.2.7"}` | `{"w": 1, "journal_requested": null}` | mongodb_w1_journal_unspecified | 2090434560 | 0.1454170336946845 | 396 |
| relaxed-pair3-mongodb | `{"architecture": "amd64", "docker_cli_sha256": "87bb910f051879cdf2a4a38e01748cbebeb27f3de962122dfdcbd0a4fbeea4ff", "exe_sha256": "d767fb752139fbc8e0e667d7e0c6fe7cdf690e0aa94a51502a32bad8c54a3672", "git_version": "8f6785264a643d2eeb5e4046f7876a23e40e1a09", "image_id": "sha256:690f1371c118a8389ecd6c82c9a7f0e37320732b1d56935a24b29dfca6933f54", "image_labels": {"org.opencontainers.image.version": "24.04"}, "kind": "mongodb_image_build", "os": "linux", "repo_digest": "mongo@sha256:7abfba0d07c9330373f8173981ea4d09cd8a82cdf0e86ccaf7008848d1d24f62", "runtime": {"cgroup_version": "2", "containerd": "2.2.1", "default_runtime": "runc", "driver": "overlay2", "engine": "29.1.3", "engine_commit": "29.1.3-0ubuntu3~22.04.2", "runc": "1.3.3-0ubuntu1~22.04.3"}, "schema_version": 1, "source_commit": "9fbea53313e85fbbf864a963ff17b0bd6dafa0be", "version": "8.2.7"}` | `{"w": 1, "journal_requested": null}` | mongodb_w1_journal_unspecified | 2204807168 | 0.1319433469325304 | 395 |
| relaxed-pair3-gomap | `{"exe_sha256": "35409b3e8a34e9af8cd578ac00ef72cb0d721abbcccfd9437eab67e050df4ee4", "expected_owner": "cow", "schema_version": 1, "source_commit": "8bea490ad5a2d89a14f02f428b4283579a6dd864", "source_dir": "/home/mikers/gomap-5335-landing-20261030/worktree", "source_patch_sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", "version": "candidate"}` | `{"w": 1, "journal_requested": null}` | command_wal_relaxed | 3183050752 | 0.15316372271627188 | 746 |
| durable-B-pair1-mongodb | `{"architecture": "amd64", "docker_cli_sha256": "87bb910f051879cdf2a4a38e01748cbebeb27f3de962122dfdcbd0a4fbeea4ff", "exe_sha256": "d767fb752139fbc8e0e667d7e0c6fe7cdf690e0aa94a51502a32bad8c54a3672", "git_version": "8f6785264a643d2eeb5e4046f7876a23e40e1a09", "image_id": "sha256:690f1371c118a8389ecd6c82c9a7f0e37320732b1d56935a24b29dfca6933f54", "image_labels": {"org.opencontainers.image.version": "24.04"}, "kind": "mongodb_image_build", "os": "linux", "repo_digest": "mongo@sha256:7abfba0d07c9330373f8173981ea4d09cd8a82cdf0e86ccaf7008848d1d24f62", "runtime": {"cgroup_version": "2", "containerd": "2.2.1", "default_runtime": "runc", "driver": "overlay2", "engine": "29.1.3", "engine_commit": "29.1.3-0ubuntu3~22.04.2", "runc": "1.3.3-0ubuntu1~22.04.3"}, "schema_version": 1, "source_commit": "9fbea53313e85fbbf864a963ff17b0bd6dafa0be", "version": "8.2.7"}` | `{"w": 1, "journal_requested": true}` | mongodb_w1_journaled | 2091360256 | 0.13536578603088856 | 398 |
| durable-B-pair1-gomap | `{"exe_sha256": "35409b3e8a34e9af8cd578ac00ef72cb0d721abbcccfd9437eab67e050df4ee4", "expected_owner": "cow", "schema_version": 1, "source_commit": "8bea490ad5a2d89a14f02f428b4283579a6dd864", "source_dir": "/home/mikers/gomap-5335-landing-20261030/worktree", "source_patch_sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", "version": "candidate"}` | `{"w": 1, "journal_requested": null}` | command_wal_durable | 3227840512 | 0.1356332516297698 | 1123 |
| durable-B-pair2-gomap | `{"exe_sha256": "35409b3e8a34e9af8cd578ac00ef72cb0d721abbcccfd9437eab67e050df4ee4", "expected_owner": "cow", "schema_version": 1, "source_commit": "8bea490ad5a2d89a14f02f428b4283579a6dd864", "source_dir": "/home/mikers/gomap-5335-landing-20261030/worktree", "source_patch_sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", "version": "candidate"}` | `{"w": 1, "journal_requested": null}` | command_wal_durable | 3342917632 | 0.13384189642965794 | 1118 |
| durable-B-pair2-mongodb | `{"architecture": "amd64", "docker_cli_sha256": "87bb910f051879cdf2a4a38e01748cbebeb27f3de962122dfdcbd0a4fbeea4ff", "exe_sha256": "d767fb752139fbc8e0e667d7e0c6fe7cdf690e0aa94a51502a32bad8c54a3672", "git_version": "8f6785264a643d2eeb5e4046f7876a23e40e1a09", "image_id": "sha256:690f1371c118a8389ecd6c82c9a7f0e37320732b1d56935a24b29dfca6933f54", "image_labels": {"org.opencontainers.image.version": "24.04"}, "kind": "mongodb_image_build", "os": "linux", "repo_digest": "mongo@sha256:7abfba0d07c9330373f8173981ea4d09cd8a82cdf0e86ccaf7008848d1d24f62", "runtime": {"cgroup_version": "2", "containerd": "2.2.1", "default_runtime": "runc", "driver": "overlay2", "engine": "29.1.3", "engine_commit": "29.1.3-0ubuntu3~22.04.2", "runc": "1.3.3-0ubuntu1~22.04.3"}, "schema_version": 1, "source_commit": "9fbea53313e85fbbf864a963ff17b0bd6dafa0be", "version": "8.2.7"}` | `{"w": 1, "journal_requested": true}` | mongodb_w1_journaled | 2147618816 | 0.13798919040709734 | 407 |
| durable-B-pair3-mongodb | `{"architecture": "amd64", "docker_cli_sha256": "87bb910f051879cdf2a4a38e01748cbebeb27f3de962122dfdcbd0a4fbeea4ff", "exe_sha256": "d767fb752139fbc8e0e667d7e0c6fe7cdf690e0aa94a51502a32bad8c54a3672", "git_version": "8f6785264a643d2eeb5e4046f7876a23e40e1a09", "image_id": "sha256:690f1371c118a8389ecd6c82c9a7f0e37320732b1d56935a24b29dfca6933f54", "image_labels": {"org.opencontainers.image.version": "24.04"}, "kind": "mongodb_image_build", "os": "linux", "repo_digest": "mongo@sha256:7abfba0d07c9330373f8173981ea4d09cd8a82cdf0e86ccaf7008848d1d24f62", "runtime": {"cgroup_version": "2", "containerd": "2.2.1", "default_runtime": "runc", "driver": "overlay2", "engine": "29.1.3", "engine_commit": "29.1.3-0ubuntu3~22.04.2", "runc": "1.3.3-0ubuntu1~22.04.3"}, "schema_version": 1, "source_commit": "9fbea53313e85fbbf864a963ff17b0bd6dafa0be", "version": "8.2.7"}` | `{"w": 1, "journal_requested": true}` | mongodb_w1_journaled | 2273796096 | 0.1311271907761693 | 398 |
| durable-B-pair3-gomap | `{"exe_sha256": "35409b3e8a34e9af8cd578ac00ef72cb0d721abbcccfd9437eab67e050df4ee4", "expected_owner": "cow", "schema_version": 1, "source_commit": "8bea490ad5a2d89a14f02f428b4283579a6dd864", "source_dir": "/home/mikers/gomap-5335-landing-20261030/worktree", "source_patch_sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", "version": "candidate"}` | `{"w": 1, "journal_requested": null}` | command_wal_durable | 3188482048 | 0.13471095450222492 | 1120 |

All attempt statuses, actual runtime/configuration and client/fixture/source identities, full operation rows, launch/cleanup/Close records, maintenance intervals, supervisor custody/resource/noise observations, original snapshot hashes and raw artifact paths/hashes are retained in `report.json`. Observation cadence overruns and external processes remain disclosed; sampled maxima are not continuous measurement. Artifact paths refer to persistent local custody and do not assert external availability. No missing sample is substituted, retried or averaged away.
