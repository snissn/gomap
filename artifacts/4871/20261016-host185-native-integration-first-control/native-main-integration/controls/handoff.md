# Integrated focused controls: first-call STOP

The first authorized call, `normal-TreeDB-db`, exited1 in 6.683542s. All30 selected top-level tests ran:29 passed and `TestPrimaryDataCertificateV6RefusalKeepsAcceptedCache` failed. Its four seeded=false/true × prepare/activate children each reported at primary_review_repairs_test.go:98:

```
late refusal not reached: observed=false err=retained metadata: original bounded cleanup work required
```

This records the earlier error and absent intended hook; its production cause is not established by this run. No repair or retry was attempted. All remaining12 calls, including all race calls and the sole M4 cell, were NOT executed.

The run recorded 74 RUN names, 69 PASS names and 5 FAIL names (including the four failed children). There were no skips. All six stable-token cases, seven owned-operation cases and four source-terminal children ran and passed. The three selected witness top-level tests passed. Exact names are in result.json.

Root selection MD f0b218c6f83248a8eb03c9c920fc73c8ba264a53f8ece9dd1fd42ec291616e7c / JSON35591560945779881a8772a4efac7beb481efd0463278b72e714aa5f7da485c9 supplied argv and deadlines unchanged. Source manifest d76289da830b8f2c856e8f0595f37a541de99368b2e582b1ae290bdab532ce9a and archive0cbcb39f0493fcca4bf42c4963f135272196cb19ee8847eb80b0729607f1fc6d were verified before launch. All10372 live hashes before and after equal the final manifest. All23 selected test-file bindings matched.

Pinned Go1.26.3 and compiler hashes are in identity.json. The runner verified Linux subreaper status. Actual PID/PGID944936 was waited; its process group and direct/adopted children are empty, with no timeout, signal or residual cleanup. Observed child process identities and actual join receipts are in normal-TreeDB-db/result.json. The runner process was also joined by its calling tool. No source/Git/index/GH changes occurred. Source/runtime lease is released.

Q32 RED/ACK0 and noAllowanceChosen remain. No actual33, native final driver/publication or performance was executed. The controls do not establish acceptance, public progress or actual receipt→source Finish ordering.
