# Exact current-head observer race excerpt

Source1bc652; hosted job113567766584; original log excerpt only. Model.Overlay map writes concurrently via coordinator dependency sync and cached leaf-manifest postwork. No flake classification or suppression.

```text
==================
WARNING: DATA RACE
Write at 0x00c00090eae0 by goroutine 8272:
  github.com/snissn/gomap/TreeDB/internal/powerlossoracle.(*Model).Overlay.func1()
      /home/runner/work/gomap/gomap/TreeDB/internal/powerlossoracle/model.go:370 +0xdd1
  path/filepath.walkDir()
      /opt/hostedtoolcache/go/1.26.8/x64/src/path/filepath/path.go:311 +0x84
  path/filepath.walkDir()
      /opt/hostedtoolcache/go/1.26.8/x64/src/path/filepath/path.go:333 +0x39e
  path/filepath.WalkDir()
      /opt/hostedtoolcache/go/1.26.8/x64/src/path/filepath/path.go:401 +0x89
  github.com/snissn/gomap/TreeDB/internal/powerlossoracle.(*Model).Overlay()
      /home/runner/work/gomap/gomap/TreeDB/internal/powerlossoracle/model.go:327 +0x1c4
  github.com/snissn/gomap/TreeDB/internal/powerlossoracle.(*Model).Observe()
      /home/runner/work/gomap/gomap/TreeDB/internal/powerlossoracle/model.go:475 +0xe44
  github.com/snissn/gomap/TreeDB_test.TestPowerLossOracleCounterexampleNewMetaMissingClosure.func1()
      /home/runner/work/gomap/gomap/TreeDB/power_loss_oracle_test.go:565 +0x106
  github.com/snissn/gomap/TreeDB/internal/durabilitycut.Emit()
      /home/runner/work/gomap/gomap/TreeDB/internal/durabilitycut/durabilitycut.go:147 +0x137
  github.com/snissn/gomap/TreeDB/db.syncStableResourceDependenciesV1()
      /home/runner/work/gomap/gomap/TreeDB/db/durable_root_runtime.go:1961 +0x1d3
  github.com/snissn/gomap/TreeDB/db.executeDurableRootStorageTransactionV1()
      /home/runner/work/gomap/gomap/TreeDB/db/durable_root_runtime.go:1830 +0x227
  github.com/snissn/gomap/TreeDB/db.(*rootPublicationRuntimeV1).Publish()
      /home/runner/work/gomap/gomap/TreeDB/db/root_publication_activation.go:1613 +0x872
  github.com/snissn/gomap/TreeDB/internal/rootpublication.(*Coordinator).run()
      /home/runner/work/gomap/gomap/TreeDB/internal/rootpublication/coordinator.go:757 +0x1077
  github.com/snissn/gomap/TreeDB/internal/rootpublication.New.gowrap1()
      /home/runner/work/gomap/gomap/TreeDB/internal/rootpublication/coordinator.go:176 +0x2e

Previous write at 0x00c00090eae0 by goroutine 8294:
  github.com/snissn/gomap/TreeDB/internal/powerlossoracle.(*Model).Overlay.func1()
      /home/runner/work/gomap/gomap/TreeDB/internal/powerlossoracle/model.go:370 +0xdd1
  path/filepath.walkDir()
      /opt/hostedtoolcache/go/1.26.8/x64/src/path/filepath/path.go:311 +0x84
  path/filepath.walkDir()
      /opt/hostedtoolcache/go/1.26.8/x64/src/path/filepath/path.go:333 +0x39e
  path/filepath.WalkDir()
      /opt/hostedtoolcache/go/1.26.8/x64/src/path/filepath/path.go:401 +0x89
  github.com/snissn/gomap/TreeDB/internal/powerlossoracle.(*Model).Overlay()
      /home/runner/work/gomap/gomap/TreeDB/internal/powerlossoracle/model.go:327 +0x1c4
  github.com/snissn/gomap/TreeDB/internal/powerlossoracle.(*Model).Observe()
      /home/runner/work/gomap/gomap/TreeDB/internal/powerlossoracle/model.go:434 +0x15c4
  github.com/snissn/gomap/TreeDB_test.TestPowerLossOracleCounterexampleNewMetaMissingClosure.func1()
      /home/runner/work/gomap/gomap/TreeDB/power_loss_oracle_test.go:565 +0x106
  github.com/snissn/gomap/TreeDB/internal/durabilitycut.EmitStablePath()
      /home/runner/work/gomap/gomap/TreeDB/internal/durabilitycut/durabilitycut.go:175 +0x239
  github.com/snissn/gomap/TreeDB/db.syncLeafGenerationManifestFile()
      /home/runner/work/gomap/gomap/TreeDB/db/leaf_generation_manifest_store.go:561 +0xa7
  github.com/snissn/gomap/TreeDB/db.(*leafGenerationManifestStore).replaceStableCompatibilityView()
      /home/runner/work/gomap/gomap/TreeDB/db/leaf_generation_manifest_store.go:531 +0x5a4
  github.com/snissn/gomap/TreeDB/db.(*leafGenerationManifestStore).replaceStable()
      /home/runner/work/gomap/gomap/TreeDB/db/leaf_generation_manifest_store.go:388 +0x1527
  github.com/snissn/gomap/TreeDB/db.(*leafGenerationManifestStore).replace()
      /home/runner/work/gomap/gomap/TreeDB/db/leaf_generation_manifest_store.go:142 +0x4a4
  github.com/snissn/gomap/TreeDB/db.(*leafGenerationManifestStore).Replace()
      /home/runner/work/gomap/gomap/TreeDB/db/leaf_generation_manifest_store.go:103 +0x98
  github.com/snissn/gomap/TreeDB/db.(*DB).replaceLeafGenerationManifest()
      /home/runner/work/gomap/gomap/TreeDB/db/leaf_generation_manifest.go:806 +0x6e
  github.com/snissn/gomap/TreeDB/db.(*DB).persistLeafGenerationManifestAndRecordLengthIndexes()
      /home/runner/work/gomap/gomap/TreeDB/db/leaf_generation_pack.go:649 +0x88
  github.com/snissn/gomap/TreeDB/db.(*DB).finalizeCommitPostWork()
      /home/runner/work/gomap/gomap/TreeDB/db/db.go:4001 +0x58b
  github.com/snissn/gomap/TreeDB/db.(*Batch).writeOptimistic()
      /home/runner/work/gomap/gomap/TreeDB/db/batch.go:639 +0x4949
  github.com/snissn/gomap/TreeDB/db.(*Batch).writeWithCommandWALIntentPreflight()
      /home/runner/work/gomap/gomap/TreeDB/db/batch.go:356 +0x75
  github.com/snissn/gomap/TreeDB/db.(*Batch).writeWithCommandWALIntent()
      /home/runner/work/gomap/gomap/TreeDB/db/batch.go:347 +0x61c
  github.com/snissn/gomap/TreeDB/db.(*Batch).write()
      /home/runner/work/gomap/gomap/TreeDB/db/batch.go:343 +0x5fe
  github.com/snissn/gomap/TreeDB/db.(*Batch).Write()
      /home/runner/work/gomap/gomap/TreeDB/db/batch.go:285 +0x28
  github.com/snissn/gomap/TreeDB/caching.(*DB).writeCanonicalFlushRunOpsChunk()
      /home/runner/work/gomap/gomap/TreeDB/caching/flush_run_planner.go:988 +0x2c4
  github.com/snissn/gomap/TreeDB/caching.(*DB).flushCanonicalPointUnitsStableIteratorStreamed.func4()
      /home/runner/work/gomap/gomap/TreeDB/caching/flush_run_planner.go:1137 +0x10e7
  github.com/snissn/gomap/TreeDB/caching.(*DB).flushCanonicalPointUnitsStableIteratorStreamed()
      /home/runner/work/gomap/gomap/TreeDB/caching/flush_run_planner.go:1181 +0xc34
  github.com/snissn/gomap/TreeDB/caching.(*DB).flushCanonicalPointUnitsStreamed()
      /home/runner/work/gomap/gomap/TreeDB/caching/flush_run_planner.go:1205 +0x276
  github.com/snissn/gomap/TreeDB/caching.(*DB).flushCanonicalPointUnits()
      /home/runner/work/gomap/gomap/TreeDB/caching/flush_run_planner.go:1421 +0x1164
  github.com/snissn/gomap/TreeDB/caching.(*DB).flushLaneOnceWithCollectionModeFrontier()
      /home/runner/work/gomap/gomap/TreeDB/caching/db.go:29948 +0x955
  github.com/snissn/gomap/TreeDB/caching.(*DB).flushCheckpointFrontierLocked.func1()
      /home/runner/work/gomap/gomap/TreeDB/caching/db.go:29373 +0x1da

Goroutine 8272 (running) created at:
  github.com/snissn/gomap/TreeDB/internal/rootpublication.New()
      /home/runner/work/gomap/gomap/TreeDB/internal/rootpublication/coordinator.go:176 +0xdca
  github.com/snissn/gomap/TreeDB/db.newRootPublicationRuntimeV1()
      /home/runner/work/gomap/gomap/TreeDB/db/root_publication_activation.go:348 +0x9ee
  github.com/snissn/gomap/TreeDB/db.(*DB).initializeRootPublicationRuntimeV1()
      /home/runner/work/gomap/gomap/TreeDB/db/root_publication_activation.go:303 +0x204
  github.com/snissn/gomap/TreeDB/db.openWithLock()
      /home/runner/work/gomap/gomap/TreeDB/db/db.go:2850 +0x3868
  github.com/snissn/gomap/TreeDB/db.Open()
      /home/runner/work/gomap/gomap/TreeDB/db/db.go:2349 +0x6a8
  github.com/snissn/gomap/TreeDB.openResolved()
      /home/runner/work/gomap/gomap/TreeDB/public.go:1097 +0x307b
  github.com/snissn/gomap/TreeDB.Open()
      /home/runner/work/gomap/gomap/TreeDB/public.go:777 +0x84
  github.com/snissn/gomap/TreeDB_test.TestPowerLossOracleCounterexampleNewMetaMissingClosure()
      /home/runner/work/gomap/gomap/TreeDB/power_loss_oracle_test.go:544 +0x19a
  testing.tRunner()
      /opt/hostedtoolcache/go/1.26.8/x64/src/testing/testing.go:2036 +0x21c
  testing.(*T).Run.gowrap1()
      /opt/hostedtoolcache/go/1.26.8/x64/src/testing/testing.go:2101 +0x38

Goroutine 8294 (running) created at:
  github.com/snissn/gomap/TreeDB/caching.(*DB).flushCheckpointFrontierLocked()
      /home/runner/work/gomap/gomap/TreeDB/caching/db.go:29368 +0x752
  github.com/snissn/gomap/TreeDB/caching.(*DB).checkpointContext()
      /home/runner/work/gomap/gomap/TreeDB/caching/db.go:25769 +0x282d
  github.com/snissn/gomap/TreeDB/caching.(*DB).Checkpoint()
      /home/runner/work/gomap/gomap/TreeDB/caching/db.go:25258 +0x99
  github.com/snissn/gomap/TreeDB.(*DB).checkpointCachedForPublicCommandWAL()
      /home/runner/work/gomap/gomap/TreeDB/public.go:2694 +0x78
  github.com/snissn/gomap/TreeDB.(*DB).Checkpoint()
      /home/runner/work/gomap/gomap/TreeDB/public.go:2685 +0x1af
  github.com/snissn/gomap/TreeDB_test.TestPowerLossOracleCounterexampleNewMetaMissingClosure()
      /home/runner/work/gomap/gomap/TreeDB/power_loss_oracle_test.go:594 +0xb2a
  testing.tRunner()
      /opt/hostedtoolcache/go/1.26.8/x64/src/testing/testing.go:2036 +0x21c
  testing.(*T).Run.gowrap1()
      /opt/hostedtoolcache/go/1.26.8/x64/src/testing/testing.go:2101 +0x38
==================
==================
WARNING: DATA RACE
Write at 0x00c00090eae0 by goroutine 8272:
  github.com/snissn/gomap/TreeDB/internal/powerlossoracle.(*Model).Overlay.func1()
      /home/runner/work/gomap/gomap/TreeDB/internal/powerlossoracle/model.go:370 +0xdd1
  path/filepath.walkDir()
      /opt/hostedtoolcache/go/1.26.8/x64/src/path/filepath/path.go:311 +0x84
  path/filepath.walkDir()
      /opt/hostedtoolcache/go/1.26.8/x64/src/path/filepath/path.go:333 +0x39e
  path/filepath.WalkDir()
      /opt/hostedtoolcache/go/1.26.8/x64/src/path/filepath/path.go:401 +0x89
  github.com/snissn/gomap/TreeDB/internal/powerlossoracle.(*Model).Overlay()
      /home/runner/work/gomap/gomap/TreeDB/internal/powerlossoracle/model.go:327 +0x1c4
```
