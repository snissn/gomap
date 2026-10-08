package db

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/durabilitycut"
	"github.com/snissn/gomap/TreeDB/page"
	"github.com/snissn/gomap/TreeDB/tree"
)

// This inspector consumes the production pager after the prefix write. It uses
// the owned loader and scalar candidate authority, never a raw candidate export.
type allocatorEmissionWitness5108 struct {
	generation, commit, parent, start, count              uint64
	branches, chunks, sentinels, reservations, retiredIDs uint64
	extents, reusedData, appendedData, abandonedPages     uint64
	freeState, retiredState                               uint64
}

func inspectAllocatorEmission5108(source freelist.PageSource, info freelist.CandidateInfoV1) (allocatorEmissionWitness5108, error) {
	var result allocatorEmissionWitness5108
	handle, err := freelist.LoadOwnedGenerationHandleV1(source, info.GenerationRef())
	if err != nil {
		return result, err
	}
	loaded, err := handle.InfoV1()
	handle.Close()
	if err != nil {
		return result, err
	}
	if loaded.Ref != info.Ref || loaded.FreePages != info.FreePages || loaded.RetiredPages != info.RetiredPages {
		return result, fmt.Errorf("loaded scalar authority differs: loaded=%+v prepared=%+v", loaded, info)
	}
	header, err := source.ReadPage(info.Ref.HeaderPageID)
	if err != nil {
		return result, err
	}
	if info.PageCount < 3 || uint64(info.PageCount) != uint64(binary.LittleEndian.Uint32(header[104:108])) {
		return result, fmt.Errorf("prepared page count %d differs from encoded header", info.PageCount)
	}
	result.generation, result.commit = info.GenerationID(), info.CommitSeq()
	result.freeState, result.retiredState = loaded.FreePages, loaded.RetiredPages
	result.parent = binary.LittleEndian.Uint64(header[48:56])
	next := binary.LittleEndian.Uint64(header[96:104])
	var targetCount int
	seen := make(map[uint64]bool)
	for next != 0 {
		if seen[next] {
			return result, fmt.Errorf("reservation cycle at %d", next)
		}
		seen[next] = true
		record, readErr := source.ReadPage(next)
		if readErr != nil {
			return result, readErr
		}
		h := page.DecodeHeader(record)
		if h.PageID != next || h.Flags != uint16(page.PageTypeFreelistReservation) ||
			binary.LittleEndian.Uint64(record[48:56]) != result.generation {
			return result, fmt.Errorf("reservation identity/type at %d", next)
		}
		result.reservations++
		for i := 0; i < int(h.Count); i++ {
			offset := 176 + 24*i
			start := binary.LittleEndian.Uint64(record[offset : offset+8])
			count := uint64(binary.LittleEndian.Uint32(record[offset+8 : offset+12]))
			result.extents++
			switch freelist.ReservationKindV1(record[offset+12]) {
			case freelist.ReservationReusedData:
				result.reusedData += count
			case freelist.ReservationAppendedData:
				result.appendedData += count
			case freelist.ReservationAbandonedAppend:
				result.abandonedPages += count
			case freelist.ReservationTargetMetadata:
				targetCount++
				result.start, result.count = start, count
			case freelist.ReservationPendingMetadataRetirement:
				result.retiredIDs += count
			}
		}
		next = binary.LittleEndian.Uint64(record[64:72])
	}
	if targetCount != 1 || result.count != uint64(info.PageCount) ||
		result.start < 2 || result.start > info.HighWater() || result.count > info.HighWater()-result.start ||
		result.retiredIDs != uint64(binary.LittleEndian.Uint32(header[108:112])) {
		return result, fmt.Errorf("target reservation differs from prepared output: %+v", result)
	}
	for id := result.start; id < result.start+result.count; id++ {
		image, readErr := source.ReadPage(id)
		if readErr != nil {
			return result, readErr
		}
		h := page.DecodeHeader(image)
		if h.PageID != id || page.CalculateChecksum(image) != h.Checksum ||
			binary.LittleEndian.Uint64(image[32:40]) != result.generation &&
				h.Flags != uint16(page.PageTypeFreelistReservation) {
			return result, fmt.Errorf("emitted page identity/checksum/generation at %d", id)
		}
		switch page.PageType(h.Flags) {
		case page.PageTypeFreelistIndex:
			if h.Count == 0 {
				result.sentinels++
			} else {
				result.branches++
			}
		case page.PageTypeFreelistChunk:
			result.chunks++
		case page.PageTypeFreelistReservation:
			if !seen[id] {
				return result, fmt.Errorf("unlinked reservation page %d", id)
			}
		case page.PageTypeFreelistGeneration:
			if id != info.Ref.HeaderPageID {
				return result, fmt.Errorf("extra generation page %d", id)
			}
		default:
			return result, fmt.Errorf("non-metadata page %d in target interval", id)
		}
	}
	if result.branches+result.chunks+result.sentinels+result.reservations+1 != result.count ||
		result.sentinels > 1 || result.sentinels != 0 && (result.branches != 0 || result.chunks != 0) {
		return result, fmt.Errorf("physical type counts do not exhaust target interval: %+v", result)
	}
	return result, nil
}

func TestFreelistPatriciaV2ActualQueuedVisibleSealEmissionAndHorizons5108(t *testing.T) {
	for _, owned := range []bool{false, true} {
		name := "legacy-manifest"
		if owned {
			name = "owned-manifest"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			options := Options{Dir: dir, Durability: DurabilityWALOffRelaxed,
				IndexOuterLeavesInValueLog: true, OwnedLeafManifests: owned,
				DisableBackgroundPrune: true}
			database, err := Open(options)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if database != nil {
					_ = database.Close()
				}
			}()
			leafLog, err := NewStandaloneLeafPageLog(dir, StandaloneLeafPageLogOptions{Compression: ValueLogCompressionOff})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if database != nil {
					_ = database.Close()
					database = nil
				}
				_ = leafLog.Close()
			}()
			database.SetLeafPageLog(leafLog)
			seed := database.NewPhysicalBatch()
			if err := seed.Set([]byte("seed"), []byte("held-before-publication")); err != nil {
				t.Fatal(err)
			}
			if err := seed.WriteSync(); err != nil {
				t.Fatal(err)
			}
			if err := seed.Close(); err != nil {
				t.Fatal(err)
			}
			held := database.AcquireSnapshot()
			defer held.Close()
			heldSequence := held.state.CommitSeq
			oldRef := database.durableRoot.record.Freelist
			oldImage, err := held.idx.pager.ReadPage(oldRef.HeaderPageID)
			if err != nil {
				t.Fatal(err)
			}
			oldImage = append([]byte(nil), oldImage...)
			expected := map[string]string{"seed": "held-before-publication"}
			type observation struct {
				phase      string
				visible    []allocatorEmissionWitness5108
				seal       allocatorEmissionWitness5108
				slotCommit [2]uint64
				profile    freelist.ResidentGenerationProfileV1
			}
			var observeMu sync.Mutex
			var phase string
			var observations []observation
			primerCaptured := make(chan struct{})
			releasePrimer := make(chan struct{})
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(releasePrimer) }) }
			defer release()
			restore := durabilitycut.Install(func(event durabilitycut.Event) error {
				if event.Root != dir || event.Point != durabilitycut.BeforePublicationSealWrite {
					return nil
				}
				runtime := database.rootPublication
				runtime.mu.Lock()
				seal := runtime.activeSeal
				if seal == nil || len(seal.prefix) < 2 || seal.prefix[len(seal.prefix)-1] != seal.prepared {
					runtime.mu.Unlock()
					return fmt.Errorf("actual public route lacks distinct visible/seal prefix")
				}
				visibleInfos := make([]freelist.CandidateInfoV1, len(seal.prefix)-1)
				var visibleErr error
				for i := range visibleInfos {
					visibleInfos[i], visibleErr = seal.prefix[i].InfoV1()
					if visibleErr != nil {
						break
					}
				}
				sealInfo, sealErr := seal.prepared.InfoV1()
				slots := seal.base.slotCommit
				runtime.mu.Unlock()
				if visibleErr != nil {
					return visibleErr
				}
				if sealErr != nil {
					return sealErr
				}
				visible := make([]allocatorEmissionWitness5108, len(visibleInfos))
				for i, info := range visibleInfos {
					var err error
					visible[i], err = inspectAllocatorEmission5108(seal.idx.pager, info)
					if err != nil {
						return fmt.Errorf("visible physical witness %d: %w", i, err)
					}
					if info.AuxiliaryCount != 0 {
						return fmt.Errorf("visible member has seal auxiliary inventory")
					}
					if i > 0 && (visible[i].parent != visible[i-1].generation ||
						visible[i].generation != visible[i-1].generation+1 || visible[i].commit != visible[i-1].commit+1) {
						return fmt.Errorf("visible member lineage changed")
					}
				}
				latest := visible[len(visible)-1]
				sealed, err := inspectAllocatorEmission5108(seal.idx.pager, sealInfo)
				if err != nil {
					return fmt.Errorf("seal physical witness: %w", err)
				}
				if sealed.generation != latest.generation+1 || sealed.parent != latest.generation ||
					sealed.commit != latest.commit || sealInfo.AuxiliaryCount < 1 {
					return fmt.Errorf("visible/seal authority fused or changed: visible=%+v seal=%+v", visible, sealed)
				}
				profile := seal.idx.allocator.ResidentGenerationProfileV1()
				if profile.RawGenerationEscaped || profile.RawLedgerEscaped || profile.RawWriterEscaped {
					return fmt.Errorf("scalar/owned witness tainted allocator ownership: %+v", profile)
				}
				observeMu.Lock()
				capturedPhase := phase
				observations = append(observations, observation{capturedPhase, visible, sealed, slots, profile})
				observeMu.Unlock()
				if capturedPhase == "queued-primer" {
					close(primerCaptured)
					<-releasePrimer
				}
				return nil
			})
			restorePending := true
			defer func() {
				if restorePending {
					restore()
				}
			}()
			verifyCapture := func(captured observation, expectedSlots [2]uint64) {
				t.Helper()
				if captured.slotCommit != expectedSlots || held.state.CommitSeq != heldSequence {
					t.Fatalf("%s changed captured slot or held horizons", captured.phase)
				}
				current, err := held.idx.pager.ReadPage(oldRef.HeaderPageID)
				if err != nil || !bytes.Equal(oldImage, current) {
					t.Fatalf("%s overwrote pinned generation header: %v", captured.phase, err)
				}
				handle, err := freelist.LoadOwnedGenerationHandleV1(held.idx.pager, oldRef)
				if err != nil {
					t.Fatalf("%s invalidated held generation: %v", captured.phase, err)
				}
				handle.Close()
				for key, value := range expected {
					got, err := database.Get([]byte(key))
					if err != nil || string(got) != value {
						t.Fatalf("current %q: %q %v", key, got, err)
					}
					got, err = held.GetAtRoot(held.state.RootPageID, []byte(key))
					if key == "seed" {
						if err != nil || string(got) != value {
							t.Fatalf("held seed %q: %q %v", key, got, err)
						}
					} else if !errors.Is(err, tree.ErrKeyNotFound) || len(got) != 0 {
						t.Fatalf("held snapshot unexpectedly contains later key %q: %q %v", key, got, err)
					}
				}
				// Persist scalar physical/class inventory after the captured cut.
				// This borrows no exporter and is not cumulative whole-call fit.
				physical := func(v allocatorEmissionWitness5108) map[string]uint64 {
					return map[string]uint64{"generation": v.generation, "commit": v.commit, "parent": v.parent,
						"metadata_start": v.start, "metadata_pages": v.count, "branches": v.branches,
						"chunks": v.chunks, "empty_sentinels": v.sentinels, "reservation_pages": v.reservations,
						"reservation_extents": v.extents, "pending_metadata_retired_ids": v.retiredIDs,
						"reused_data_pages": v.reusedData, "appended_data_pages": v.appendedData,
						"abandoned_append_pages": v.abandonedPages, "free_state_pages": v.freeState,
						"retired_state_pages": v.retiredState}
				}
				visibleInventory := make([]map[string]uint64, len(captured.visible))
				for i, v := range captured.visible {
					visibleInventory[i] = physical(v)
				}
				inventory, marshalErr := json.Marshal(map[string]interface{}{"phase": captured.phase,
					"visible": visibleInventory, "seal": physical(captured.seal), "slot_commits": captured.slotCommit,
					"held_commit": heldSequence, "resident_conservative": captured.profile,
					"whole_caller_finite_fit": false, "cumulative_birth_census": false})
				if marshalErr != nil {
					t.Fatal(marshalErr)
				}
				t.Logf("L5108_INVENTORY %s", inventory)
				t.Logf("phase=%s visible=%+v seal=%+v slot_horizons=%v held_sequence=%d resident=%+v",
					captured.phase, captured.visible, captured.seal, captured.slotCommit, heldSequence, captured.profile)
			}
			run := func(label string, members int, apply func() error) {
				t.Helper()
				slots := database.durableRoot.slotCommit
				observeMu.Lock()
				phase = label
				before := len(observations)
				observeMu.Unlock()
				if err := apply(); err != nil {
					t.Fatal(err)
				}
				observeMu.Lock()
				if len(observations) != before+1 {
					observeMu.Unlock()
					t.Fatalf("%s did not publish exactly one seal", label)
				}
				captured := observations[before]
				observeMu.Unlock()
				if len(captured.visible) != members {
					t.Fatalf("%s visible member count=%d want=%d", label, len(captured.visible), members)
				}
				verifyCapture(captured, slots)
			}
			for round := 0; round < 3; round++ {
				run(fmt.Sprintf("native-group-%d", round), 1, func() error {
					group, err := database.BeginRootPublicationBuildGroup()
					if err != nil {
						return err
					}
					defer group.Close()
					for i := 0; i < 2; i++ {
						key, value := fmt.Sprintf("group/%d/%d", round, i), fmt.Sprintf("value/%d/%d", round, i)
						batch := database.NewPhysicalBatch().(*Batch)
						if err := batch.Set([]byte(key), []byte(value)); err != nil {
							_ = batch.Close()
							return err
						}
						if err := batch.SetRootPublicationBuildGroup(group, i == 1); err != nil {
							_ = batch.Close()
							return err
						}
						if i == 0 {
							err = batch.Write()
						} else {
							err = batch.WriteSync()
						}
						closeErr := batch.Close()
						if err != nil {
							return err
						}
						if closeErr != nil {
							return closeErr
						}
						expected[key] = value
					}
					return nil
				})
			}
			// A real ordinary queue needs more than two private builders.
			// Hold one already-prepared publication at the existing I/O cut;
			// foreground activation is admitted while Publish performs I/O.
			// Two standalone writes then create an actual visible debt prefix
			// before the coordinator can prepare its next seal.
			beforeSlots, beforeSlot := database.durableRoot.slotCommit, database.durableRoot.slot
			observeMu.Lock()
			phase = "queued-primer"
			queueBefore := len(observations)
			observeMu.Unlock()
			writeRow := func(key, value string, syncWrite bool) error {
				batch := database.NewPhysicalBatch()
				if err := batch.Set([]byte(key), []byte(value)); err != nil {
					_ = batch.Close()
					return err
				}
				var err error
				if syncWrite {
					err = batch.WriteSync()
				} else {
					err = batch.Write()
				}
				closeErr := batch.Close()
				if err != nil {
					return err
				}
				return closeErr
			}
			primerDone := make(chan error, 1)
			go func() { primerDone <- writeRow("queue/primer", "primer", true) }()
			join := func(done <-chan error, label string) {
				t.Helper()
				select {
				case err := <-done:
					if err != nil {
						t.Fatalf("%s: %v", label, err)
					}
				case <-time.After(10 * time.Second):
					t.Fatalf("%s did not join", label)
				}
			}
			select {
			case <-primerCaptured:
			case err := <-primerDone:
				t.Fatalf("primer returned before capture: %v", err)
			case <-time.After(10 * time.Second):
				t.Fatal("primer did not reach existing publication cut")
			}
			expected["queue/primer"] = "primer"
			activatedBefore := database.rootPublication.coordinator.Stats().VisibleCommitSeq
			observeMu.Lock()
			phase = "queued-two-ordinary-visible"
			observeMu.Unlock()
			if err := writeRow("queue/ordinary", "ordinary", false); err != nil {
				release()
				join(primerDone, "primer")
				t.Fatal(err)
			}
			expected["queue/ordinary"] = "ordinary"
			syncDone := make(chan error, 1)
			go func() { syncDone <- writeRow("queue/sync", "sync", true) }()
			deadline := time.NewTimer(10 * time.Second)
			poll := time.NewTicker(time.Millisecond)
			queued := false
			for !queued {
				select {
				case err := <-syncDone:
					release()
					join(primerDone, "primer")
					t.Fatalf("sync returned before held publication released: %v", err)
				case <-deadline.C:
					release()
					join(primerDone, "primer")
					join(syncDone, "sync")
					t.Fatal("two ordinary visible candidates were not activated")
				case <-poll.C:
					queued = database.rootPublication.coordinator.Stats().VisibleCommitSeq == activatedBefore+2
				}
			}
			deadline.Stop()
			poll.Stop()
			release()
			join(primerDone, "primer")
			join(syncDone, "sync")
			expected["queue/sync"] = "sync"
			observeMu.Lock()
			queueCaptured := append([]observation(nil), observations[queueBefore:]...)
			observeMu.Unlock()
			if len(queueCaptured) != 2 || len(queueCaptured[0].visible) != 1 || len(queueCaptured[1].visible) != 2 {
				t.Fatalf("controlled public queue captured wrong visible/seal shape: %+v", queueCaptured)
			}
			verifyCapture(queueCaptured[0], beforeSlots)
			afterPrimerSlots := beforeSlots
			afterPrimerSlots[beforeSlot^1] = queueCaptured[0].seal.commit
			verifyCapture(queueCaptured[1], afterPrimerSlots)
			if owned {
				run("public-owned-manifest-checkpoint", 1, func() error {
					_, err := database.CheckpointOwnedLeafManifest()
					return err
				})
			}
			restore()
			restorePending = false
			if err := held.Close(); err != nil {
				t.Fatal(err)
			}
			if err := database.Close(); err != nil {
				t.Fatal(err)
			}
			database = nil
			reopened, err := Open(options)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			for key, value := range expected {
				got, err := reopened.Get([]byte(key))
				if err != nil || string(got) != value {
					t.Fatalf("reopened %q: %q %v", key, got, err)
				}
			}
		})
	}
}
