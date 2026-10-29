# Integrated source binding supplement

**ACCEPT — formatting integration confirmed for the same nine-path source scope.** The original `independent-review.md` is unchanged, SHA-256 `45cb26fdfc359c0aaad56b4915ec9d5c33bedefb74d28a472ca8a2d9b28d8429`.

Canonical HEAD is `49baf47d6c3748467e08845caff0ede779f7f9c6`. Independently hashed `evidence/native-write-failure-repair1/capacity-integrated-manifest.json`: `a40292482956b0425598875c345ccf5c65f85f566f0499954a5903ad37f24450`. Its donor manifest binding remains `dc2fb5caf4f8d212a2fd678cc0e4eb05ae15534c3f739a9d981f31ca96077739`.

Verified every donor file against its frozen after-hash and every canonical file against the integrated manifest. The documentation is byte-identical. For all eight Go files, a read-only Python lexical comparison preserving comments, quoted/raw literals, identifiers and maximal operator tokens found identical token streams after excluding inter-token whitespace and explicit statement semicolons expanded by formatting. No source construction or Go tool was used.

| Canonical selected path | Verified SHA-256 |
| --- | --- |
| `TreeDB/db/native_semantic_state.go` | `34719275931ef9787c0ef4f3cc9a007bc8a07bb925e69b8689ae307d278fbbdf` |
| `TreeDB/caching/cow_native_txn.go` | `c93489418d3cd108394943955e04efea0cbc1a2c87734681963b1bb4e08079bb` |
| `TreeDB/collections/native_existing.go` | `c3e9af37041e24c0647c37588b89a6eb20b7bb866c05c11533e85431d32b7a51` |
| `TreeDB/collections/native_json_crud.go` | `348a3b23e9a8999d420f50ed6561963d406a04852c16c3537db3eb2fe4a6601e` |
| `TreeDB/docs/spec/cow-cache-publication.md` | `0e10a04f5df097ba5d7242fcca0f4313cc4b23b21fb843c193e3bba7709140b5` |
| `TreeDB/collections/native_producer_gate.go` | `44e172cfbfeefb761506e42a37f7bef742080782066981ae93335f6d0d85f663` |
| `TreeDB/caching/cow_native_observation_test.go` | `adc2f625c57da234784f1aeca1ce955f030183e274d422692d4cb80564027182` |
| `TreeDB/collections/native_producer_gate_test.go` | `f4a8ed906e8a5b383c71b06d1414ee29b8387c63c6121e23ba0a227b34d957d5` |
| `TreeDB/documentservice/ordinary_columngraph_native_concurrent_write_test.go` | `4d254c8e95f682f6f00b595c541a22e43dd2f1111237c9085d1d1b9c48a465c0` |

The original source-review disposition and limits apply to these integrated bytes. Root reports intended original-public capacity RED; the integrated manifest retains log SHA `20c3b1ee7d7c73afa1cf6a5b5b53d45e8d7fa9749f5f6343468cd05458f9ba15`. That runtime was not independently executed or audited in this formatting-only supplement. No GREEN, race, performance or product acceptance is inferred. No Go/build/gofmt/tests/runtime/GitHub or canonical edits were performed.
