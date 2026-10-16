package mvcc

import (
	"bytes"
	"errors"
	"testing"
	"unsafe"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/iterator"
	"github.com/snissn/gomap/TreeDB/internal/mvcckey"
	"github.com/snissn/gomap/TreeDB/internal/retainedalloc"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

// This is a previsibility safety control, not the native success gate. The
// retained route must never ACK a selected history row at its missing seam.
func TestNativeMVCCRealBackendRouteRefusalPreservesCustodyV6(t *testing.T) {
	testNativeMVCCRealBackendRouteCustodyV6(t, "promoted")
}

func TestNativeMVCCCopiedParentAcrossBothDurableSlotsV6(t *testing.T) {
	for _, mode := range []string{"cancel-parent", "stale-parent"} {
		t.Run(mode, func(t *testing.T) { testNativeMVCCRealBackendRouteCustodyV6(t, mode) })
	}
}

func testNativeMVCCRealBackendRouteCustodyV6(t *testing.T, mode string) {
	opts := treedb.OptionsFor(treedb.ProfileNoWALFast, t.TempDir())
	opts.IndexOuterLeavesInValueLog = false
	opts.ValueLog.PointerThreshold = 2048
	opts.BackgroundCheckpointInterval = -1
	d, err := treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	s := New(d)
	key := []byte("actual-selected-anchor")
	for ts := uint64(1); ts <= 33; ts++ {
		if err := s.CommitAt(ts, []Mutation{{Key: key, Value: []byte{byte(ts)}}}, CommitRelaxed); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.AdvanceDiscardFloor(32, CommitDurable); err != nil {
		t.Fatal(err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatal(err)
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d, err = treedb.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	s = New(d)
	oldKey, _ := mvcckey.Encode(key, 1)
	oldValue, err := d.Get(oldKey)
	if err != nil {
		t.Fatal(err)
	}
	oldReader := d.AcquireSnapshot()
	if oldReader == nil {
		t.Fatal("actual old reader missing")
	}
	defer oldReader.Close()
	var c *PruneCursor
	defer func() {
		if c != nil {
			if err := c.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	var lastPhase uint8 = 255
	for calls := 0; calls < 10000; calls++ {
		stats, err := s.PruneVersions(PruneOptions{Mode: CommitDurable, BatchSize: 1, WorkRecords: 32, WorkBytes: 1 << 20, Cursor: c})
		c = stats.Cursor
		if stats.WorkRecords > 32 || stats.WorkBytes > 1<<20 || stats.Pruned != 0 || stats.Complete {
			t.Fatalf("unqualified route acknowledged work: %+v err=%v", stats, err)
		}
		if c == nil {
			t.Fatalf("missing original cursor: %v", err)
		}
		if c.phase != lastPhase {
			t.Logf("call=%d phase=%d work=%d/%d selected=%d anchor=%d requestFloor=%d", calls, c.phase, stats.WorkRecords, stats.WorkBytes, len(c.batchEntries), c.anchorSize, c.request.Floor())
			lastPhase = c.phase
		}
		if mode != "promoted" && c.preparation != nil {
			sealStage, promoted := c.preparation.MaintenanceNativeSealScalarsForTest()
			if sealStage > 0 && sealStage < 9 && !promoted {
				t.Logf("copied parent retained before promotion: calls=%d stage=%d work=%d/%d", calls+1, sealStage, stats.WorkRecords, stats.WorkBytes)
				// Two actual durable writes move both physical slots. The raw
				// copied parent is evidence and has acquired no parent-root edge.
				for ts := uint64(34); ts <= 35; ts++ {
					if err := s.CommitAt(ts, []Mutation{{Key: []byte("newer-real-writer"), Value: []byte{byte(ts)}}}, CommitDurable); err != nil {
						t.Fatal("foreground publication while parent is private", ts, err)
					}
				}
				if mode == "stale-parent" {
					stale := false
					for turn := 0; turn < 100; turn++ {
						w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
						ready, e := c.preparation.StepNativeMVCCSealPreparationV6(&w)
						if w.Records > 32 || w.Bytes > 1<<20 || ready {
							t.Fatalf("stale copied parent acquired promotion: ready=%v err=%v work=%+v", ready, e, w)
						}
						if e != nil {
							t.Logf("stale copied parent refused before promotion: stage=%d records=%d bytes=%d err=%v", sealStage, w.Records, w.Bytes, e)
							stale = true
							break
						}
					}
					_, promoted = c.preparation.MaintenanceNativeSealScalarsForTest()
					if !stale || promoted {
						t.Fatal("copied parent did not refuse before acquiring old authority")
					}
				}
				if got, e := d.Get(oldKey); e != nil || !bytes.Equal(got, oldValue) {
					t.Fatal("private parent preparation changed selected DATA", got, e)
				}
				if err := c.Close(); err != nil {
					t.Fatal("finite stale/private-parent cancellation", err)
				}
				c = nil
				newKey, _ := mvcckey.Encode([]byte("newer-real-writer"), 35)
				newValue, e := d.Get(newKey)
				if e != nil || len(newValue) == 0 {
					t.Fatal("cancel damaged newest DATA", e)
				}
				if e := oldReader.Close(); e != nil {
					t.Fatal(e)
				}
				if e := d.Checkpoint(); e != nil {
					t.Fatal(e)
				}
				if e := d.Close(); e != nil {
					t.Fatal(e)
				}
				d, e = treedb.Open(opts)
				if e != nil {
					t.Fatal(e)
				}
				defer d.Close()
				if got, e := d.Get(newKey); e != nil || !bytes.Equal(got, newValue) {
					t.Fatalf("reopen after both-slot advancement lost newest DATA: %q %v", got, e)
				}
				return
			}
		}
		if err == nil {
			continue
		}
		t.Logf("first actual route refusal: calls=%d phase=%d error=%v", calls+1, c.phase, err)
		if !errors.Is(err, rootpublication.ErrNativeMVCCRootWorkRequiredV6) {
			t.Fatal(err)
		}
		if c.phase != 26 || c.preparation == nil || len(c.batchEntries) != 1 || !c.haveAnchor || c.request.Floor() != 32 {
			t.Fatalf("refused before real selected+anchor producer: phase=%d", c.phase)
		}
		stage, bound, candidate := c.preparation.MaintenanceNativeCandidateScalarsForTest()
		if stage != 8 || !bound || !candidate {
			t.Fatalf("actual member/candidate not reached: stage=%d bound=%v candidate=%v", stage, bound, candidate)
		}
		sealStage, promoted := c.preparation.MaintenanceNativeSealScalarsForTest()
		if sealStage != 13 || !promoted {
			t.Fatalf("actual private promotion not reached: stage=%d promoted=%v", sealStage, promoted)
		}
		if count, same := c.preparation.MaintenanceNativeIndexCaptureScalarsForTest(); count != 2 || !same {
			t.Fatalf("actual two original index operands not retained: count=%d sameGeneration=%v", count, same)
		}
		testNativePreEffectGeometryV6(t, c)

		if got, e := d.Get(oldKey); e != nil || !bytes.Equal(got, oldValue) {
			t.Fatalf("refused output became visible: %x %v", got, e)
		}
		if got, e := oldReader.Get(oldKey); e != nil || !bytes.Equal(got, oldValue) {
			t.Fatalf("old reader changed: %x %v", got, e)
		}
		if err := s.CommitAt(34, []Mutation{{Key: []byte("newer-real-writer"), Value: []byte("durable")}}, CommitDurable); err != nil {
			t.Fatalf("foreground writer after retained refusal: %v", err)
		}
		if ready, err := c.preparation.StepNativeMVCCAllocatorV6(&iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}); ready || !errors.Is(err, freelist.ErrCOWPreparationStaleV6) {
			t.Fatalf("newer real writer did not revoke private source: ready=%v err=%v", ready, err)
		}
		newKey, _ := mvcckey.Encode([]byte("newer-real-writer"), 34)
		newValue, err := d.Get(newKey)
		if err != nil || len(newValue) == 0 {
			t.Fatal("missing actual newer DATA output", err)
		}
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
		c = nil
		if got, err := d.Get(newKey); err != nil || !bytes.Equal(got, newValue) {
			t.Fatalf("native stale cancellation damaged newer durable output: %q %v", got, err)
		}
		if err := oldReader.Close(); err != nil {
			t.Fatal(err)
		}
		if err := d.Checkpoint(); err != nil {
			t.Fatal(err)
		}
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
		d, err = treedb.Open(opts)
		if err != nil {
			t.Fatal(err)
		}
		defer d.Close()
		if got, err := d.Get(newKey); err != nil || !bytes.Equal(got, newValue) {
			t.Fatalf("reopen lost newer durable output after stale cancellation: %q %v", got, err)
		}
		return
	}
	t.Fatalf("real route did not reach finite seam: phase=%d", c.phase)
}

// Source-bound scalar observations at the original missing-publication seam.
// Each probe uses its own unchanged Q32/1MiB work; no grant drives publication.
func testNativePreEffectGeometryV6(t *testing.T, c *PruneCursor) {
	t.Helper()
	w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	basis, ready := c.preparation.MaintenanceNativeBasisGeometryForTestV6(&w)
	t.Logf("native-observer group=1 sourceSymbol=db.MaintenanceNativeBasisGeometryForTestV6 geometry=%+v ready=%v work=%d/%d", basis, ready, w.Records, w.Bytes)
	if !ready {
		t.Log("native-observer incomplete=mutable-basis-observation-exceeds-original-Q32 noAllowance=true")
		return
	}
	if !basis.SameIndex || !basis.SameExpectedSequence || !basis.SameParent || !basis.SameRecord || !basis.PublicationOperandsReady || basis.CandidatePhase != 8 || basis.SealPhase != 13 {
		t.Fatalf("pre-effect basis changed: %+v", basis)
	}
	testNativeObserverBasisV6(t, c, basis, "group1")
	w = iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	allocator, aReady := c.preparation.MaintenanceNativeAllocatorGeometryForTestV6(&w)
	t.Logf("native-observer group=2 sourceSymbol=freelist.claimDataAdmissionLockedV6+activationAdmissionV6 geometry=%+v ready=%v work=%d/%d", allocator, aReady, w.Records, w.Bytes)
	if aReady && (!allocator.SourceMatches || !allocator.PreVisible || !allocator.PageSizesValid) {
		t.Fatalf("pre-effect allocator basis changed: %+v", allocator)
	}
	w = iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	remainder, rReady := c.preparation.MaintenanceNativeRemainderGeometryForTestV6(&w)
	t.Logf("native-observer group=2 sourceSymbol=freelist.NativePublicationBoundWithWorkV6+db.nativePublicationRemainderForGeometryV6 geometry=%+v ready=%v work=%d/%d", remainder, rReady, w.Records, w.Bytes)
	w = iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	coordinator, coReady := c.preparation.MaintenanceNativeCoordinatorGeometryForTestV6(&w)
	t.Logf("native-observer group=2 sourceSymbol=rootpublication.MaintenanceNativeGeometryForTestV6 geometry=%+v ready=%v work=%d/%d runtimeServiceGC=true", coordinator, coReady, w.Records, w.Bytes)
	if coReady && (coordinator.Pending != 0 || coordinator.Builders != 0 || coordinator.Publishing || !coordinator.ExpectedReceiptHead || !basis.PublicationBound || !basis.SamePublicationCoordinator) {
		t.Fatalf("pre-effect coordinator changed: %+v", coordinator)
	}
	testNativeObserverBasisV6(t, c, basis, "group2")
	var totalR, totalB, k, keyBytes, requestBytes uint64
	complete := aReady && !allocator.Overflow && rReady && remainder.Complete && coReady && coordinator.RuntimeEvidenceSupported && basis.FullBasisReady
	for ordinal := uint64(0); ; ordinal++ {
		w = iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		g, ok := c.preparation.MaintenanceNativeKeyGeometryForTestV6(ordinal, c.request, &w)
		t.Logf("native-observer group=3 sourceSymbol=mvccadmission.Request.matchesLocked+db.observeNativePublicationCutV6 ordinal=%d geometry=%+v ready=%v work=%d/%d", ordinal, g, ok, w.Records, w.Bytes)
		if !ok || !g.RequestMatches || !g.SameRequest {
			complete = false
			break
		}
		requestBytes = g.RequestValidateBytes
		if g.End {
			break
		}
		if !w.Reserve(1, 32) || k == ^uint64(0) || g.KeyBytes > ^uint64(0)-keyBytes {
			complete = false
			break
		}
		k++
		keyBytes += g.KeyBytes
	}
	// Seven DB fixed/source expression rows. They evaluate actual Reserve byte
	// operands; neither this arithmetic nor these observation grants authorize F.
	for role := uint8(0); role < 7; role++ {
		w = iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		g, ok := c.preparation.MaintenanceNativeFRowForTestV6(role, basis.Debt, k, keyBytes, requestBytes, remainder, &w)
		t.Logf("native-observer group=3 sourceSymbol=db.MaintenanceNativeFRowForTestV6 role=%d operation=%+v ready=%v work=%d/%d", role, g, ok, w.Records, w.Bytes)
		if !ok || !g.Complete || !testNativeObserverAddV6(&w, &totalR, &totalB, g.Records, g.Bytes) {
			complete = false
		}
	}
	w = iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	dispatchR, dispatchB, ok := rootpublication.NativeActivationDispatchPriceForTestV6(&w)
	if !ok || !testNativeObserverAddV6(&w, &totalR, &totalB, dispatchR, dispatchB) {
		complete = false
	}
	t.Logf("native-observer group=3 sourceSymbol=rootpublication.reserveDurableRootCallbackDispatchV6 operation=%d/%d ready=%v work=%d/%d", dispatchR, dispatchB, ok, w.Records, w.Bytes)
	// The four coordinator rows include BOTH completion branches, error-view
	// traffic, clocks, timer upper branch, actual notifications and builder unwind.
	w = iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	if !testNativeObserverAddV6(&w, &totalR, &totalB, coordinator.CompletionRecords, coordinator.CompletionBytes) || !testNativeObserverAddV6(&w, &totalR, &totalB, coordinator.BuilderReleaseRecords, coordinator.BuilderReleaseBytes) || !testNativeObserverAddV6(&w, &totalR, &totalB, coordinator.AcquireRecords, coordinator.AcquireBytes) || !testNativeObserverAddV6(&w, &totalR, &totalB, coordinator.EnqueueRecords, coordinator.EnqueueBytes) {
		complete = false
	}
	w = iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	z, zReady := c.preparation.MaintenanceNativeSnapshotPriceForTestV6(&w)
	t.Logf("native-observer group=3 sourceSymbol=db.beginSnapshotPublicationV6 geometry=%+v ready=%v work=%d/%d", z, zReady, w.Records, w.Bytes)
	if !zReady || !z.Complete || z.Overflow || !testNativeObserverAddV6(&w, &totalR, &totalB, z.Records, z.Bytes) {
		complete = false
	}
	w = iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	if !aReady || !testNativeObserverAddV6(&w, &totalR, &totalB, allocator.ClaimRecords, allocator.ClaimBytes) {
		complete = false
	}
	var vR, vB uint64
	framesComplete := aReady
	for ordinal := uint64(0); ; ordinal++ {
		w = iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		g, ok := c.preparation.MaintenanceNativeFrameGeometryForTestV6(ordinal, allocator, &w)
		t.Logf("native-observer group=3+4 sourceSymbol=zipper.ValidateCOWInstalledPagesV6+freelist.ValidateInstalledPageV6 ordinal=%d geometry=%+v ready=%v work=%d/%d chargeIncludesAllFrames=true opsBackingBorrowed=true", ordinal, g, ok, w.Records, w.Bytes)
		if !ok || g.Overflow {
			framesComplete = false
			break
		}
		if ordinal == 0 {
			if !testNativeObserverAddV6(&w, &vR, &vB, 1, 2*g.HeaderRaw) {
				framesComplete = false
				break
			}
		}
		if g.End {
			break
		}
		if !g.Written || !g.PageResult || g.Adopted || g.AppendTarget || g.Diagnostic || !testNativeObserverAddV6(&w, &vR, &vB, g.ValidationRecords, g.ValidationBytes) {
			framesComplete = false
			break
		}
	}
	w = iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	if !framesComplete || !testNativeObserverAddV6(&w, &totalR, &totalB, vR, vB) || !rReady || !remainder.Complete || !testNativeObserverAddV6(&w, &totalR, &totalB, remainder.Records, remainder.Bytes) {
		complete = false
	}
	t.Logf("native-observer group=3 expression=F293+D+2K+23P+8M+3S+N+Z+V+A K=%d keyBytes=%d D=%d P=%d M=%d S=%d Nupper=%d Z=%d V=%d A=%d computedRecords=%d computedBytes=%d applicableComplete=%v observationsAreNotAtomic=true noAllowanceChosen=true publishedGCOutputs=2 privatePrebornHeaders=0", k, keyBytes, basis.Debt, remainder.Bound.Prefix, remainder.Bound.MetadataPages, remainder.Seals, remainder.Bound.ClaimedIDs, z.Records, vR, allocator.ClaimRecords, totalR, totalB, complete)
	testNativeObserverBasisV6(t, c, basis, "group3")
	w = iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	resources, resReady := c.preparation.MaintenanceNativeResourcesGeometryForTestV6(&w)
	t.Logf("native-observer group=4 sourceSymbol=rootpublication.MaintenanceNativeEmptyGeometryForTestV6 geometry=%+v ready=%v work=%d/%d separateSetAllocations=true sharedKindAndDirectoryCountOnce=true", resources, resReady, w.Records, w.Bytes)
	for index := 0; index < 2; index++ {
		w = iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		target, first, last, pages := uint64(0), uint64(0), uint64(0), uint64(0)
		if index == 0 {
			target, first, last, pages = allocator.RequiredHighWater, allocator.MetadataFirst, allocator.MetadataLast, allocator.MetadataPages
		}
		g, ok := c.preparation.MaintenanceNativeIndexGeometryForTestV6(index, target, first, last, pages, &w)
		t.Logf("native-observer group=4 sourceSymbol=db.nativeStablePagerCaptureV6.constructOwnedOperationsV6+pager.NativeFileFenceWorkV6 index=%d geometry=%+v ready=%v work=%d/%d", index, g, ok, w.Records, w.Bytes)
		if ok && (!g.SameSnapshot || !g.SameNativeCallback || !g.SameSelectedPager || !g.SameTokenOperations || !g.SameRetainedCapture || !g.SameMetadata || !g.SameRegistry || !g.SamePhysicalOwner || !g.CustodyPresent || g.CustodyCharge == 0 || g.CustodyComplete) {
			t.Fatalf("original index identity changed: %+v", g)
		}
	}
	for role := uint8(0); role < 13; role++ {
		w = iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		g, ok := c.preparation.MaintenanceNativeRetainedRowForTestV6(role, &w)
		t.Logf("native-observer group=4 sourceSymbol=db.MaintenanceNativeRetainedRowForTestV6 role=%d geometry=%+v ready=%v work=%d/%d", role, g, ok, w.Records, w.Bytes)
	}
	for role := uint8(0); role < 5; role++ {
		w = iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		g, ok := c.preparation.MaintenanceNativeSetRowForTestV6(role, &w)
		t.Logf("native-observer group=4 sourceSymbol=rootpublication.MaintenanceNativeSetAllocationForTestV6 role=%d geometry=%+v ready=%v work=%d/%d dedupBasis=candidateResources", role, g, ok, w.Records, w.Bytes)
	}
	w = iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	manifest, sameManifest, manifestReady := c.preparation.MaintenanceNativeManifestRowForTestV6(&w)
	t.Logf("native-observer group=4 sourceSymbol=rootpublication.DependencyManifestV1 geometry=%+v sameSeal=%v ready=%v work=%d/%d", manifest, sameManifest, manifestReady, w.Records, w.Bytes)
	w = iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	transaction, transactionReady := c.preparation.MaintenanceNativeTransactionRowForTestV6(&w)
	t.Logf("native-observer group=4 sourceSymbol=rootpublication.primaryTransactionConstructionV6 geometry=%+v ready=%v work=%d/%d transactionAndBankEnvelopesRequireTransferDedup=true", transaction, transactionReady, w.Records, w.Bytes)
	for role := uint8(0); role < 4; role++ {
		w = iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		g, ok := c.preparation.MaintenanceNativeRegistryTableForTestV6(role, &w)
		t.Logf("native-observer group=4 sourceSymbol=rootpublication.registryTable role=%d geometry=%+v ready=%v work=%d/%d backingOnlyNotPayloadGraph=true", role, g, ok, w.Records, w.Bytes)
	}
	w = iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	sink, sinkReady := c.preparation.MaintenanceNativeSinkGeometryForTestV6(&w)
	t.Logf("native-observer group=4 sourceSymbol=freelist.COWPreparationWorkV6.sinkPrivate geometry=%+v ready=%v work=%d/%d backingProvenance=explicitPrivateCapacity privateValidationControl=true", sink, sinkReady, w.Records, w.Bytes)
	for role := uint8(0); role < 3; role++ {
		w = iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		g, same, ok := c.capture.MaintenanceNativeOwnerGeometryForTestV6(c.preparation, role, &w)
		t.Logf("native-observer group=4 domain=Owner-envelope sourceSymbol=retainedalloc.Owner.MaintenanceNativeOwnerGeometryForTestV6 role=%d samePrimary=%v geometry=%+v ready=%v work=%d/%d doNotAddPhysicalRows=true", role, same, g, ok, w.Records, w.Bytes)
		w = iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
		bank, sameSource, sameParent, ok := c.preparation.MaintenanceNativeBankGeometryForTestV6(role, &w)
		t.Logf("native-observer group=4 domain=bank-envelope sourceSymbol=primaryarena.MaintenanceNativeBankGeometryForTestV6 role=%d sameSource=%v sameParent=%v geometry=%+v ready=%v work=%d/%d sameBankImageNotAdditionalAllocation=true", role, sameSource, sameParent, bank, ok, w.Records, w.Bytes)
	}
	w = iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	capture, capReady := c.capture.MaintenanceNativeCaptureGeometryForTestV6(&w)
	t.Logf("native-observer group=4 sourceSymbol=caching.NativeMVCCCaptureV6 geometry=%+v ready=%v work=%d/%d", capture, capReady, w.Records, w.Bytes)
	w = iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	lease, leaseReady := c.capture.MaintenanceNativeExternalGeometryForTestV6(c.retention, &w)
	if w.Reserve(2, 2*uint64(unsafe.Sizeof(c.retainedBytes)+unsafe.Sizeof(c.batchEntries)+unsafe.Sizeof(c.batchPayloadCharge))+128) {
		t.Logf("native-observer group=4 domain=policy-reservation sourceSymbol=mvcc.PruneCursor+memtable.COWExternalLease cursorRaw=%d cursorRounded=%d retainedPolicyBytes=%d batchCapacity=%d batchPayloadCharge=%d lease=%+v ready=%v work=%d/%d embeddedPreparedAndCapturesNotSeparateAllocations=true", unsafe.Sizeof(*c), retainedalloc.AllocationCharge(uint64(unsafe.Sizeof(*c))), c.retainedBytes, cap(c.batchEntries), c.batchPayloadCharge, lease, leaseReady, w.Records, w.Bytes)
	} else {
		t.Logf("native-observer group=4 domain=policy-reservation incomplete=outer-cursor-observer-traffic-exceeds-Q32-or-1MiB ready=false work=%d/%d", w.Records, w.Bytes)
	}
	testNativeSelectedConstructorLedgerV6(t, c)
	testNativeObserverBasisV6(t, c, basis, "group4")
	t.Log("native-observer group=5 retainedUnionComplete=false namedIncomplete=nonempty-resource-logical-graphs,selected-constructor-unsupported-rows,current-owner-observations-not-atomic futureFinish=FutureGeometryUnobserved; phase7-backing-RemovalWork+BindFinish+alias-clear -> original-ContinueFinish/Retire -> post.oldState-root-and-Set-transfer -> real-release-queue/node/dependency/lastowner -> both-original-index-capture-Snapshot-and-custody -> original-Set-close -> output/input/private-scratch -> enrollment-and-outer-lease-last-alias -> original-terminal-token; hardIO=sticky-whole-original-frame-and-receipt-with-incomplete-Close/Stop noTransitiveHeapClaim=true noAllowance=true Q32=RED ACK=0")
	testNativeObserverBasisV6(t, c, basis, "group5")
}

func testNativeObserverAddV6(w *iterator.OrdinalScanWork, r, b *uint64, records, bytes uint64) bool {
	if w == nil || !w.Reserve(1, 64) || records > ^uint64(0)-*r || bytes > ^uint64(0)-*b {
		return false
	}
	*r += records
	*b += bytes
	return true
}
func testNativeObserverBasisV6(t *testing.T, c *PruneCursor, before backenddb.NativeBasisGeometryForTestV6, group string) {
	t.Helper()
	w := iterator.OrdinalScanWork{RecordLimit: 32, ByteLimit: 1 << 20}
	after, ready := c.preparation.MaintenanceNativeBasisGeometryForTestV6(&w)
	t.Logf("native-observer basisAfter=%s ready=%v same=%v work=%d/%d wholeGraphAtomic=false", group, ready, after == before, w.Records, w.Bytes)
	if !ready || after != before || c.phase != 26 {
		t.Fatalf("pre-effect probe changed original basis after %s: before=%+v after=%+v ready=%v phase=%d", group, before, after, ready, c.phase)
	}
}
