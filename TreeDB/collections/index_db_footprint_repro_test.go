package collections

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/freelist"
	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
	"github.com/snissn/gomap/TreeDB/node"
	"github.com/snissn/gomap/TreeDB/page"
)

type rawIndexPageSource struct {
	data []byte
}

func (s rawIndexPageSource) pageCount() uint64 {
	return uint64(len(s.data) / page.PageSize)
}

func (s rawIndexPageSource) ReadPage(pageID uint64) ([]byte, error) {
	off := int(pageID) * page.PageSize
	if off < 0 || off+page.PageSize > len(s.data) {
		return nil, fmt.Errorf("page %d out of bounds (total=%d)", pageID, s.pageCount())
	}
	return s.data[off : off+page.PageSize], nil
}

type forensicRootStats struct {
	name                string
	rootID              uint64
	pages               uint64
	leafPages           uint64
	internalPages       uint64
	emptyLeafPages      uint64
	nonFirstEmptyLeaves uint64
	under10PctLeafPages uint64
	under25PctLeafPages uint64
	leafFillSum         float64
	minPageID           uint64
	maxPageID           uint64
	hasPages            bool
}

func (r forensicRootStats) avgLeafFillPct() float64 {
	if r.leafPages == 0 {
		return 0
	}
	return (r.leafFillSum / float64(r.leafPages)) * 100.0
}

type forensicSlotInfo struct {
	valid     bool
	slot      uint64
	meta      page.DurableMetaV1
	record    rootpublication.DurableRootRecordV1
	reachable map[uint64]struct{}
	userPages uint64
	sysPages  uint64
	colPages  uint64
	colRoots  []forensicRootStats
}

type forensicIndexCensus struct {
	fileBytes          int64
	totalPages         uint64
	selectedSlot       uint64
	slots              [2]forensicSlotInfo
	slotsSameRoots     bool
	unionReachable     uint64
	freelistFree       uint64
	freelistRetired    uint64
	metaAndAuxPages    uint64
	unaccountedPages   uint64
	unaccountedMinPage uint64
	unaccountedMaxPage uint64
	unaccountedByType  map[page.PageType]uint64
	allColMinPage      uint64
	allColMaxPage      uint64
	allColHasPages     bool
	postingBlocksStats forensicRootStats
	termsStats         forensicRootStats
	totalEmptyLeaves   uint64
	totalNonFirstEmpty uint64
	totalColLeaves     uint64
}

func inspectIndexDBForensics(t *testing.T, indexPath string) forensicIndexCensus {
	t.Helper()
	raw, err := os.ReadFile(indexPath)
	if err != nil {
		t.Fatalf("read %s: %v", indexPath, err)
	}
	src := rawIndexPageSource{data: raw}
	out := forensicIndexCensus{
		fileBytes:         int64(len(raw)),
		totalPages:        src.pageCount(),
		unaccountedByType: make(map[page.PageType]uint64),
	}

	walkTree := func(rootID uint64, visited map[uint64]struct{}, stats *forensicRootStats) {
		if rootID == 0 || rootID >= src.pageCount() {
			return
		}
		var walk func(id uint64, isFirstChild bool)
		walk = func(id uint64, isFirstChild bool) {
			if id == 0 || id >= src.pageCount() {
				return
			}
			if _, ok := visited[id]; ok {
				return
			}
			visited[id] = struct{}{}
			img, err := src.ReadPage(id)
			if err != nil {
				return
			}
			n := node.NewNode(img)
			if stats != nil {
				stats.pages++
				if !stats.hasPages {
					stats.minPageID = id
					stats.maxPageID = id
					stats.hasPages = true
				} else {
					if id < stats.minPageID {
						stats.minPageID = id
					}
					if id > stats.maxPageID {
						stats.maxPageID = id
					}
				}
			}
			switch n.Type() {
			case page.PageTypeLeaf:
				if stats != nil {
					stats.leafPages++
					cnt := n.Count()
					if cnt == 0 {
						stats.emptyLeafPages++
						if !isFirstChild {
							stats.nonFirstEmptyLeaves++
						}
					}
					used := page.PageSize - n.FreeSpace()
					fill := float64(used) / float64(page.PageSize)
					stats.leafFillSum += fill
					if used*10 < page.PageSize {
						stats.under10PctLeafPages++
					}
					if used*4 < page.PageSize {
						stats.under25PctLeafPages++
					}
				}
			case page.PageTypeInternal:
				if stats != nil {
					stats.internalPages++
				}
				cnt := n.Count()
				for i := uint16(0); i < cnt; i++ {
					_, childRef, err := n.GetInternalEntryRefView(i)
					if err != nil {
						continue
					}
					if childRef.Kind == page.ChildRefLeafLog {
						continue
					}
					walk(childRef.Page, i == 0)
				}
			}
		}
		walk(rootID, true)
	}

	collectDescriptors := func(sysRoot uint64) map[string][]uint64 {
		descs := make(map[string][]uint64)
		if sysRoot == 0 || sysRoot >= src.pageCount() {
			return descs
		}
		seen := make(map[uint64]struct{})
		var walk func(id uint64)
		walk = func(id uint64) {
			if id == 0 || id >= src.pageCount() {
				return
			}
			if _, ok := seen[id]; ok {
				return
			}
			seen[id] = struct{}{}
			img, err := src.ReadPage(id)
			if err != nil {
				return
			}
			n := node.NewNode(img)
			switch n.Type() {
			case page.PageTypeLeaf:
				for i := uint16(0); i < n.Count(); i++ {
					k, v, _, flags, err := n.GetLeafEntryView(i)
					if err != nil || flags&node.FlagTombstone != 0 || flags&node.FlagPointer != 0 {
						continue
					}
					ks := string(k)
					if strings.HasPrefix(ks, "collections/root/") && len(v) == 8 {
						descs[ks] = []uint64{binary.BigEndian.Uint64(v)}
					} else if strings.HasPrefix(ks, "collections/root-overlay/") && len(v)%8 == 0 {
						ids := make([]uint64, len(v)/8)
						for j := range ids {
							ids[j] = binary.BigEndian.Uint64(v[j*8 : (j+1)*8])
						}
						descs[ks] = ids
					}
				}
			case page.PageTypeInternal:
				for i := uint16(0); i < n.Count(); i++ {
					_, childRef, err := n.GetInternalEntryRefView(i)
					if err != nil || childRef.Kind == page.ChildRefLeafLog {
						continue
					}
					walk(childRef.Page)
				}
			}
		}
		walk(sysRoot)
		return descs
	}

	var bestSeq uint64
	for slot := uint64(0); slot < 2; slot++ {
		img, err := src.ReadPage(slot)
		if err != nil || !page.VerifyChecksumNonMutating(img) {
			continue
		}
		meta, err := page.DecodeDurableMetaV1(img[page.PageHeaderSize:])
		if err != nil || meta.CommitSeq == 0 {
			continue
		}
		recImg, err := src.ReadPage(meta.RootRecordPageID)
		if err != nil {
			continue
		}
		rec, err := rootpublication.DecodeDurableRootRecordV1(recImg, meta.RootRecordPageID, meta.RootRecordDigest)
		if err != nil {
			continue
		}
		info := forensicSlotInfo{
			valid:     true,
			slot:      slot,
			meta:      meta,
			record:    rec,
			reachable: make(map[uint64]struct{}),
		}
		userVisited := make(map[uint64]struct{})
		walkTree(rec.UserRootPageID, userVisited, nil)
		info.userPages = uint64(len(userVisited))
		for id := range userVisited {
			info.reachable[id] = struct{}{}
		}
		sysVisited := make(map[uint64]struct{})
		walkTree(rec.SystemRootPageID, sysVisited, nil)
		info.sysPages = uint64(len(sysVisited))
		for id := range sysVisited {
			info.reachable[id] = struct{}{}
		}
		descs := collectDescriptors(rec.SystemRootPageID)
		keys := make([]string, 0, len(descs))
		for k := range descs {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		colVisited := make(map[uint64]struct{})
		for _, k := range keys {
			for _, rid := range descs[k] {
				if rid == 0 {
					continue
				}
				rs := forensicRootStats{name: k, rootID: rid}
				rootVisited := make(map[uint64]struct{})
				walkTree(rid, rootVisited, &rs)
				for id := range rootVisited {
					colVisited[id] = struct{}{}
					info.reachable[id] = struct{}{}
				}
				info.colRoots = append(info.colRoots, rs)
			}
		}
		info.colPages = uint64(len(colVisited))
		out.slots[slot] = info
		if meta.CommitSeq > bestSeq {
			bestSeq = meta.CommitSeq
			out.selectedSlot = slot
		}
	}

	if out.slots[0].valid && out.slots[1].valid {
		r0 := out.slots[0].record
		r1 := out.slots[1].record
		out.slotsSameRoots = r0.UserRootPageID == r1.UserRootPageID && r0.SystemRootPageID == r1.SystemRootPageID
	}

	unionReachable := make(map[uint64]struct{})
	for s := 0; s < 2; s++ {
		if !out.slots[s].valid {
			continue
		}
		for id := range out.slots[s].reachable {
			unionReachable[id] = struct{}{}
		}
	}
	out.unionReachable = uint64(len(unionReachable))

	sel := out.slots[out.selectedSlot]
	for _, rs := range sel.colRoots {
		out.totalColLeaves += rs.leafPages
		out.totalEmptyLeaves += rs.emptyLeafPages
		out.totalNonFirstEmpty += rs.nonFirstEmptyLeaves
		if rs.hasPages {
			if !out.allColHasPages {
				out.allColMinPage = rs.minPageID
				out.allColMaxPage = rs.maxPageID
				out.allColHasPages = true
			} else {
				if rs.minPageID < out.allColMinPage {
					out.allColMinPage = rs.minPageID
				}
				if rs.maxPageID > out.allColMaxPage {
					out.allColMaxPage = rs.maxPageID
				}
			}
		}
		if strings.Contains(rs.name, "text-v2-posting-blocks") {
			out.postingBlocksStats = rs
		}
		if strings.Contains(rs.name, "text-v2-terms") {
			out.termsStats = rs
		}
	}

	var gen *freelist.FreelistGenerationV1
	if sel.valid {
		var err error
		gen, err = freelist.LoadGenerationV1(src, sel.record.Freelist)
		if err != nil {
			t.Fatalf("LoadGenerationV1(slot=%d): %v", out.selectedSlot, err)
		}
		out.freelistFree = gen.FreeCount()
		out.freelistRetired = gen.RetiredCount()
	}

	// Exact page-by-page accounting across [0 .. totalPages-1].
	// Note: pages within [0 .. HighWater-1] are managed by the COW freelist;
	// pages >= HighWater (if any) are preallocated tail chunk padding.
	highWater := out.totalPages
	if gen != nil && gen.HighWater() < highWater {
		highWater = gen.HighWater()
	}
	for id := uint64(0); id < highWater; id++ {
		if id < 2 {
			out.metaAndAuxPages++
			continue
		}
		if _, ok := unionReachable[id]; ok {
			continue
		}
		if gen != nil && gen.Allocatable(id) {
			continue
		}
		if gen != nil {
			unused, err := gen.SnapshotPageUnusedV1(id, ^uint64(0))
			if err == nil && unused {
				continue
			}
		}
		img, err := src.ReadPage(id)
		if err != nil {
			continue
		}
		pt := node.NewNode(img).Type()
		switch pt {
		case page.PageTypeFreelistGeneration,
			page.PageTypeFreelistIndex,
			page.PageTypeFreelistChunk,
			page.PageTypeFreelistReservation,
			page.PageTypeDependencyManifest,
			page.PageTypeDurableRootRecord:
			out.metaAndAuxPages++
			continue
		}
		out.unaccountedPages++
		out.unaccountedByType[pt]++
		if out.unaccountedPages == 1 || id < out.unaccountedMinPage {
			out.unaccountedMinPage = id
		}
		if id > out.unaccountedMaxPage {
			out.unaccountedMaxPage = id
		}
	}

	return out
}

func formatForensicCensus(label string, c forensicIndexCensus, rep backenddb.IndexVacuumTriggerReport, plan backenddb.CompactStorageStats) string {
	var b strings.Builder
	fmt.Fprintf(&b, "[%s] index.db=%d bytes (TotalPages=%d, HighWater=%d) selectedSlot=%d slotsSameRoots=%v\n",
		label, c.fileBytes, c.totalPages, c.slots[c.selectedSlot].record.TotalPages, c.selectedSlot, c.slotsSameRoots)
	fmt.Fprintf(&b, "  slot0: valid=%v seq=%d userRoot=%d sysRoot=%d reachable=%d (user=%d sys=%d col=%d) flFree=%d flRet=%d\n",
		c.slots[0].valid, c.slots[0].record.CommitSeq, c.slots[0].record.UserRootPageID, c.slots[0].record.SystemRootPageID,
		len(c.slots[0].reachable), c.slots[0].userPages, c.slots[0].sysPages, c.slots[0].colPages,
		c.slots[0].record.FreelistFreeCount, c.slots[0].record.FreelistRetiredCount)
	fmt.Fprintf(&b, "  slot1: valid=%v seq=%d userRoot=%d sysRoot=%d reachable=%d (user=%d sys=%d col=%d) flFree=%d flRet=%d\n",
		c.slots[1].valid, c.slots[1].record.CommitSeq, c.slots[1].record.UserRootPageID, c.slots[1].record.SystemRootPageID,
		len(c.slots[1].reachable), c.slots[1].userPages, c.slots[1].sysPages, c.slots[1].colPages,
		c.slots[1].record.FreelistFreeCount, c.slots[1].record.FreelistRetiredCount)
	fmt.Fprintf(&b, "  accounting: unionReachable=%d freelistFree=%d freelistRetired=%d metaAux=%d UNACCOUNTED=%d (range=[%d..%d] types=%v)\n",
		c.unionReachable, c.freelistFree, c.freelistRetired, c.metaAndAuxPages,
		c.unaccountedPages, c.unaccountedMinPage, c.unaccountedMaxPage, c.unaccountedByType)
	fmt.Fprintf(&b, "  collection range=[%d..%d] totalLeaves=%d emptyLeaves=%d nonFirstEmpty=%d\n",
		c.allColMinPage, c.allColMaxPage, c.totalColLeaves, c.totalEmptyLeaves, c.totalNonFirstEmpty)
	fmt.Fprintf(&b, "  posting-blocks: pages=%d leaves=%d empty=%d (nonFirst=%d) <10%%=%d <25%%=%d avgFill=%.1f%% range=[%d..%d]\n",
		c.postingBlocksStats.pages, c.postingBlocksStats.leafPages, c.postingBlocksStats.emptyLeafPages,
		c.postingBlocksStats.nonFirstEmptyLeaves, c.postingBlocksStats.under10PctLeafPages,
		c.postingBlocksStats.under25PctLeafPages, c.postingBlocksStats.avgLeafFillPct(),
		c.postingBlocksStats.minPageID, c.postingBlocksStats.maxPageID)
	fmt.Fprintf(&b, "  terms:          pages=%d leaves=%d empty=%d (nonFirst=%d) <10%%=%d <25%%=%d avgFill=%.1f%% range=[%d..%d]\n",
		c.termsStats.pages, c.termsStats.leafPages, c.termsStats.emptyLeafPages,
		c.termsStats.nonFirstEmptyLeaves, c.termsStats.under10PctLeafPages,
		c.termsStats.under25PctLeafPages, c.termsStats.avgLeafFillPct(),
		c.termsStats.minPageID, c.termsStats.maxPageID)
	fmt.Fprintf(&b, "  triggerReport: TotalPages=%d UserPages=%d ColPages=%d ColSpan=%d ColSpanRatioPPM=%d ColLeaves=%d FLReclaimable=%d PlanVacReq=%v PlanVacReason=%q\n",
		rep.TotalPages, rep.UserPages, rep.CollectionRootPages, rep.CollectionRootSpan,
		rep.CollectionRootSpanRatioPPM, rep.CollectionRootLeafPages, rep.FreelistReclaimablePages,
		plan.RemainingDebt.IndexVacuumRequired, plan.RemainingDebt.IndexVacuumReason)
	return b.String()
}

func makeFootprintDoc(docIdx, cycle int) []byte {
	// Generate deterministic text with:
	// - shared corpus tokens (creating multi-block posting lists)
	// - cycle-scoped churn tokens (creating terms/posting blocks deleted on update/rewrite)
	// - document-specific rare tokens (growing text-v2-terms across many leaves)
	var b bytes.Buffer
	b.WriteString(`{"body":"`)
	fmt.Fprintf(&b, "anchor_common_%02d shared_topic_%03d ", docIdx%16, (docIdx+cycle)%128)
	if docIdx%20 == 0 {
		b.WriteString("phrase_alpha phrase_beta ")
	}
	for j := 0; j < 24; j++ {
		termID := (docIdx*31 + j*17 + cycle*13) % 2400
		fmt.Fprintf(&b, "c%02d_term_%04d ", cycle, termID)
	}
	for j := 0; j < 10; j++ {
		fmt.Fprintf(&b, "uniq_c%02d_d%05d_t%02d ", cycle, docIdx, j)
	}
	fmt.Fprintf(&b, `stable_doc_%05d"}`, docIdx)
	return b.Bytes()
}

func TestMaintainTextIndexes_IndexDBFootprintRepro(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("online vacuum unsupported on windows")
	}
	ctx := context.Background()
	dir := t.TempDir()

	opts := treedb.OptionsFor(treedb.ProfileNoWALFast, dir)
	opts.IndexOuterLeavesInValueLog = false
	opts.BackgroundCheckpointInterval = -1
	opts.BackgroundCheckpointIdleDuration = -1

	d, cleanup, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		t.Fatalf("OpenBackendWithCachedLeafLog: %v", err)
	}
	manager := NewCollectionManager(d)
	closed := false
	closeAll := func() {
		if closed {
			return
		}
		closed = true
		if err := manager.Close(); err != nil {
			t.Errorf("manager.Close: %v", err)
		}
		if err := cleanup(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}
	t.Cleanup(closeAll)

	meta := &CollectionMeta{
		Name: "repo_chunks",
		Options: CollectionOptions{
			DataRootStoragePolicy:   RootStoragePager,
			IndexStateStoragePolicy: RootStoragePager,
		},
		TextIndexes: []TextIndexDefinition{
			{
				Name:           "lexical",
				Version:        TextIndexVersionV2,
				StorePositions: true,
				StoragePolicy:  RootStoragePager,
				Fields:         []TextIndexField{{Field: "body"}},
			},
		},
	}
	if _, err := manager.CreateCollection(meta); err != nil {
		t.Fatalf("CreateCollection: %v", err)
	}
	col, err := manager.OpenCollection("repo_chunks")
	if err != nil {
		t.Fatalf("OpenCollection: %v", err)
	}

	const (
		docCount  = 1600
		batchSize = 100
		cycles    = 5
	)
	indexPath := filepath.Join(d.Dir(), "index.db")

	// Initial ingest.
	for start := 0; start < docCount; start += batchSize {
		end := min(start+batchSize, docCount)
		ids := make([][]byte, 0, end-start)
		docs := make([][]byte, 0, end-start)
		for i := start; i < end; i++ {
			ids = append(ids, []byte(fmt.Sprintf("doc-%05d", i)))
			docs = append(docs, makeFootprintDoc(i, 0))
		}
		if _, err := col.InsertBatch(ids, docs); err != nil {
			t.Fatalf("initial InsertBatch[%d:%d]: %v", start, end, err)
		}
	}
	if err := manager.FlushAll(); err != nil {
		t.Fatalf("FlushAll initial: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("Checkpoint initial: %v", err)
	}
	rep0, err := d.IndexVacuumTriggerReport()
	if err != nil {
		t.Fatalf("IndexVacuumTriggerReport initial: %v", err)
	}
	plan0, err := d.CompactStoragePlan(ctx, backenddb.CompactStorageOptions{})
	if err != nil {
		t.Fatalf("CompactStoragePlan initial: %v", err)
	}
	c0 := inspectIndexDBForensics(t, indexPath)
	t.Log(formatForensicCensus("initial-ingest", c0, rep0, plan0))

	liveIDs := make([]int, docCount)
	for i := 0; i < docCount; i++ {
		liveIDs[i] = i
	}
	nextNewDoc := docCount

	for cycle := 1; cycle <= cycles; cycle++ {
		// Update 50% of live docs in deterministic batches (churning posting blocks & terms).
		var updItems []UpdateBatchItem
		for _, id := range liveIDs {
			if (id+cycle)%2 == 0 {
				docBytes := makeFootprintDoc(id, cycle)
				updItems = append(updItems, UpdateBatchItem{
					DocumentID: []byte(fmt.Sprintf("doc-%05d", id)),
					Update: func([]byte) ([]byte, bool, error) {
						return docBytes, true, nil
					},
				})
				if len(updItems) >= docCount/2 {
					break
				}
			}
		}
		for start := 0; start < len(updItems); start += batchSize {
			end := min(start+batchSize, len(updItems))
			if _, err := col.UpdateBatch(updItems[start:end]); err != nil {
				t.Fatalf("cycle %d UpdateBatch[%d:%d]: %v", cycle, start, end, err)
			}
		}
		// Delete 20% of docs and insert fresh replacement docs deterministically.
		var delIDs [][]byte
		var keptIDs []int
		for _, id := range liveIDs {
			if (id+cycle)%5 == 0 && len(delIDs) < docCount/5 {
				delIDs = append(delIDs, []byte(fmt.Sprintf("doc-%05d", id)))
				continue
			}
			keptIDs = append(keptIDs, id)
		}
		for start := 0; start < len(delIDs); start += batchSize {
			end := min(start+batchSize, len(delIDs))
			if _, err := col.DeleteBatch(delIDs[start:end]); err != nil {
				t.Fatalf("cycle %d DeleteBatch[%d:%d]: %v", cycle, start, end, err)
			}
		}
		insIDs := make([][]byte, 0, len(delIDs))
		insDocs := make([][]byte, 0, len(delIDs))
		for range delIDs {
			id := nextNewDoc
			nextNewDoc++
			insIDs = append(insIDs, []byte(fmt.Sprintf("doc-%05d", id)))
			insDocs = append(insDocs, makeFootprintDoc(id, cycle))
			keptIDs = append(keptIDs, id)
		}
		liveIDs = keptIDs
		for start := 0; start < len(insIDs); start += batchSize {
			end := min(start+batchSize, len(insIDs))
			if _, err := col.InsertBatch(insIDs[start:end], insDocs[start:end]); err != nil {
				t.Fatalf("cycle %d InsertBatch[%d:%d]: %v", cycle, start, end, err)
			}
		}
		if err := manager.FlushAll(); err != nil {
			t.Fatalf("cycle %d FlushAll: %v", cycle, err)
		}

		maintStats, err := manager.MaintainTextIndexes(ctx, TextIndexMaintenanceOptions{
			Force:                true,
			RunStorageCompaction: true,
		})
		if err != nil {
			t.Fatalf("cycle %d MaintainTextIndexes: %v", cycle, err)
		}
		if err := d.Checkpoint(); err != nil {
			t.Fatalf("cycle %d Checkpoint: %v", cycle, err)
		}

		rep, err := d.IndexVacuumTriggerReport()
		if err != nil {
			t.Fatalf("cycle %d IndexVacuumTriggerReport: %v", cycle, err)
		}
		plan, err := d.CompactStoragePlan(ctx, backenddb.CompactStorageOptions{})
		if err != nil {
			t.Fatalf("cycle %d CompactStoragePlan: %v", cycle, err)
		}
		census := inspectIndexDBForensics(t, indexPath)
		var vacPhases []string
		if maintStats.StorageCompaction != nil {
			for _, p := range maintStats.StorageCompaction.Storage.Phases {
				if strings.Contains(p.Name, "vacuum") {
					vacPhases = append(vacPhases, fmt.Sprintf("%s(req=%v status=%s reason=%s skipped=%v)", p.Name, p.Required, p.Status, p.Reason, p.Skipped))
				}
			}
		}
		t.Logf("cycle %d maintain: rewritten=%d compacted=%v vacPhases=%v", cycle, maintStats.IndexesRewritten, maintStats.StorageCompacted, vacPhases)
		t.Log(formatForensicCensus(fmt.Sprintf("after-cycle-%d", cycle), census, rep, plan))
	}

	// Explicit Gate 1 probe: VacuumIndexOnline -> inspect immediately -> CompactStorage (refreshDurableRootFallback) -> inspect again.
	// First, ensure slot0 and slot1 differ via one small update + checkpoint.
	{
		id := liveIDs[0]
		key := []byte(fmt.Sprintf("doc-%05d", id))
		newDoc := makeFootprintDoc(id, cycles+1)
		if _, _, err := col.Update(key, func([]byte) ([]byte, bool, error) {
			return newDoc, true, nil
		}); err != nil {
			t.Fatalf("pre-probe Update: %v", err)
		}
	}
	if err := manager.FlushAll(); err != nil {
		t.Fatalf("pre-probe FlushAll: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("pre-probe Checkpoint: %v", err)
	}
	repPreVac, _ := d.IndexVacuumTriggerReport()
	planPreVac, _ := d.CompactStoragePlan(ctx, backenddb.CompactStorageOptions{})
	cPreVac := inspectIndexDBForensics(t, indexPath)
	t.Log(formatForensicCensus("probe-before-VacuumIndexOnline(distinct-slots)", cPreVac, repPreVac, planPreVac))

	var mBeforeVac1, mAfterVac1 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&mBeforeVac1)
	vac1Start := time.Now()
	vacStats1, err := d.VacuumIndexOnlineWithStats(ctx)
	vac1Dur := time.Since(vac1Start)
	runtime.ReadMemStats(&mAfterVac1)
	if err != nil {
		t.Fatalf("VacuumIndexOnlineWithStats #1: %v", err)
	}
	repPostVac1, _ := d.IndexVacuumTriggerReport()
	planPostVac1, _ := d.CompactStoragePlan(ctx, backenddb.CompactStorageOptions{})
	cPostVac1 := inspectIndexDBForensics(t, indexPath)
	t.Logf("VacuumIndexOnline #1 stats: dur=%v B=%d allocs=%d OlderRootRebuilds=%d OlderRootRebuiltPages=%d ReplacementPagerPages=%d",
		vac1Dur, mAfterVac1.TotalAlloc-mBeforeVac1.TotalAlloc, mAfterVac1.Mallocs-mBeforeVac1.Mallocs,
		vacStats1.OlderRootRebuilds, vacStats1.OlderRootRebuiltPages, vacStats1.ReplacementPagerPages)
	t.Log(formatForensicCensus("probe-after-VacuumIndexOnline-1(before-fallback-refresh)", cPostVac1, repPostVac1, planPostVac1))

	// Now run CompactStorage (which triggers checkpoint + refreshDurableRootFallback, or run a write+checkpoint to supersede slot^1).
	compactAfterVac, err := manager.CompactStorage(ctx, backenddb.CompactStorageOptions{})
	if err != nil {
		t.Fatalf("CompactStorage after VacuumIndexOnline #1: %v", err)
	}
	repPostCS, _ := d.IndexVacuumTriggerReport()
	planPostCS, _ := d.CompactStoragePlan(ctx, backenddb.CompactStorageOptions{})
	cPostCS := inspectIndexDBForensics(t, indexPath)
	t.Logf("CompactStorage after vac1: FullyCompacted=%v PolicyFullyCompacted=%v RemainingDebt.IndexVacuumRequired=%v Reason=%q",
		compactAfterVac.Storage.FullyCompacted, compactAfterVac.Storage.PolicyFullyCompacted,
		compactAfterVac.Storage.RemainingDebt.IndexVacuumRequired, compactAfterVac.Storage.RemainingDebt.IndexVacuumReason)
	t.Log(formatForensicCensus("probe-after-CompactStorage-post-vac1", cPostCS, repPostCS, planPostCS))

	// Now do ONE small update + Checkpoint to force the older slot (slot^1) from VacuumIndexOnline #1 to be overwritten!
	{
		id := liveIDs[1]
		key := []byte(fmt.Sprintf("doc-%05d", id))
		newDoc := makeFootprintDoc(id, cycles+2)
		if _, _, err := col.Update(key, func([]byte) ([]byte, bool, error) {
			return newDoc, true, nil
		}); err != nil {
			t.Fatalf("post-vac1 Update: %v", err)
		}
	}
	if err := manager.FlushAll(); err != nil {
		t.Fatalf("post-vac1 FlushAll: %v", err)
	}
	if err := d.Checkpoint(); err != nil {
		t.Fatalf("post-vac1 Checkpoint: %v", err)
	}
	repPostSupersede, _ := d.IndexVacuumTriggerReport()
	planPostSupersede, _ := d.CompactStoragePlan(ctx, backenddb.CompactStorageOptions{})
	cPostSupersede := inspectIndexDBForensics(t, indexPath)
	t.Log(formatForensicCensus("probe-after-superseding-slot^1-via-next-commit", cPostSupersede, repPostSupersede, planPostSupersede))

	// Also probe VacuumIndexOnline when slot0 and slot1 ALREADY share identical roots (after CompactStorage/refreshDurableRootFallback).
	var mBeforeCSEx, mAfterCSEx runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&mBeforeCSEx)
	csExStart := time.Now()
	csExStats, err := manager.CompactStorage(ctx, backenddb.CompactStorageOptions{Mode: backenddb.CompactStorageExhaustive})
	csExDur := time.Since(csExStart)
	runtime.ReadMemStats(&mAfterCSEx)
	if err != nil {
		t.Fatalf("CompactStorage Exhaustive: %v", err)
	}
	t.Logf("CompactStorage Exhaustive stats: dur=%v B=%d allocs=%d FullyCompacted=%v PolicyFullyCompacted=%v ByteMinimized=%v RemainingVacReq=%v Reason=%q",
		csExDur, mAfterCSEx.TotalAlloc-mBeforeCSEx.TotalAlloc, mAfterCSEx.Mallocs-mBeforeCSEx.Mallocs,
		csExStats.Storage.FullyCompacted, csExStats.Storage.PolicyFullyCompacted, csExStats.Storage.ByteMinimized,
		csExStats.Storage.RemainingDebt.IndexVacuumRequired, csExStats.Storage.RemainingDebt.IndexVacuumReason)
	repPreVac2, _ := d.IndexVacuumTriggerReport()
	planPreVac2, _ := d.CompactStoragePlan(ctx, backenddb.CompactStorageOptions{})
	cPreVac2 := inspectIndexDBForensics(t, indexPath)
	t.Log(formatForensicCensus("probe-after-Exhaustive-CompactStorage", cPreVac2, repPreVac2, planPreVac2))

	vacStats2, err := d.VacuumIndexOnlineWithStats(ctx)
	if err != nil {
		t.Fatalf("VacuumIndexOnlineWithStats #2: %v", err)
	}
	repPostVac2, _ := d.IndexVacuumTriggerReport()
	planPostVac2, _ := d.CompactStoragePlan(ctx, backenddb.CompactStorageOptions{})
	cPostVac2 := inspectIndexDBForensics(t, indexPath)
	t.Logf("VacuumIndexOnline #2 stats: OlderRootRebuilds=%d OlderRootRebuiltPages=%d ReplacementPagerPages=%d",
		vacStats2.OlderRootRebuilds, vacStats2.OlderRootRebuiltPages, vacStats2.ReplacementPagerPages)
	t.Log(formatForensicCensus("probe-after-VacuumIndexOnline-2(identical-input-slots)", cPostVac2, repPostVac2, planPostVac2))

	// Measure post-vacuum SearchText & PhraseSearch latency and verify parity across reopen.
	const queryIters = 50
	var searchBefore TextSearchResponse
	var mBeforeSearch, mAfterSearch runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&mBeforeSearch)
	searchStart := time.Now()
	for i := 0; i < queryIters; i++ {
		searchBefore, err = col.SearchText(TextSearchOptions{IndexName: "lexical", Query: "anchor_common_00", TopK: 20})
		if err != nil {
			t.Fatalf("SearchText iter %d: %v", i, err)
		}
	}
	searchDur := time.Since(searchStart)
	runtime.ReadMemStats(&mAfterSearch)

	var phraseBefore TextSearchResponse
	var mBeforePhrase, mAfterPhrase runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&mBeforePhrase)
	phraseStart := time.Now()
	for i := 0; i < queryIters; i++ {
		phraseBefore, err = col.SearchText(TextSearchOptions{IndexName: "lexical", Phrase: &TextSearchPhraseQuery{Query: "phrase_alpha phrase_beta"}, TopK: 20})
		if err != nil {
			t.Fatalf("PhraseSearch iter %d: %v", i, err)
		}
	}
	phraseDur := time.Since(phraseStart)
	runtime.ReadMemStats(&mAfterPhrase)
	t.Logf("post-vacuum query perf (%d iters): SearchText=%v/op (%d B/op, %d allocs/op, hits=%d) PhraseSearch=%v/op (%d B/op, %d allocs/op, hits=%d)",
		queryIters,
		searchDur/queryIters, (mAfterSearch.TotalAlloc-mBeforeSearch.TotalAlloc)/queryIters, (mAfterSearch.Mallocs-mBeforeSearch.Mallocs)/queryIters, len(searchBefore.Results),
		phraseDur/queryIters, (mAfterPhrase.TotalAlloc-mBeforePhrase.TotalAlloc)/queryIters, (mAfterPhrase.Mallocs-mBeforePhrase.Mallocs)/queryIters, len(phraseBefore.Results))

	// Target footprint assertions (RED on baseline 1aea6c2d0, GREEN after G2 fixes).
	if cPostSupersede.unaccountedPages > 0 {
		t.Errorf("unaccounted (leaked) pages after superseding VacuumIndexOnline slot^1: got %d (range=[%d..%d], types=%v), want 0",
			cPostSupersede.unaccountedPages, cPostSupersede.unaccountedMinPage, cPostSupersede.unaccountedMaxPage, cPostSupersede.unaccountedByType)
	}
	if cPreVac2.unaccountedPages > 0 {
		t.Errorf("unaccounted (leaked) pages after Exhaustive CompactStorage: got %d (range=[%d..%d], types=%v), want 0",
			cPreVac2.unaccountedPages, cPreVac2.unaccountedMinPage, cPreVac2.unaccountedMaxPage, cPreVac2.unaccountedByType)
	}
	if cPreVac2.totalNonFirstEmpty > 0 {
		t.Errorf("non-first empty leaves remain after MaintainTextIndexes + CompactStorage: got %d, want 0", cPreVac2.totalNonFirstEmpty)
	}
	if cPreVac2.postingBlocksStats.leafPages > 0 && cPreVac2.postingBlocksStats.avgLeafFillPct() < 75.0 {
		t.Errorf("posting-blocks avg leaf fill too low after compaction: got %.1f%%, want >= 75.0%%", cPreVac2.postingBlocksStats.avgLeafFillPct())
	}
	if cPreVac2.termsStats.leafPages > 0 && cPreVac2.termsStats.avgLeafFillPct() < 75.0 {
		t.Errorf("terms avg leaf fill too low after compaction: got %.1f%%, want >= 75.0%%", cPreVac2.termsStats.avgLeafFillPct())
	}
	if planPreVac2.RemainingDebt.IndexVacuumRequired {
		t.Errorf("Exhaustive CompactStorage did not converge: RemainingDebt.IndexVacuumRequired=true reason=%q", planPreVac2.RemainingDebt.IndexVacuumReason)
	}

	if len(searchBefore.Results) == 0 {
		t.Fatalf("expected non-empty SearchText results before reopen")
	}
	if len(phraseBefore.Results) == 0 {
		t.Fatalf("expected non-empty PhraseSearch results before reopen")
	}

	closeAll()

	d2, cleanup2, err := treedb.OpenBackendWithCachedLeafLog(opts)
	if err != nil {
		t.Fatalf("reopen OpenBackendWithCachedLeafLog: %v", err)
	}
	m2 := NewCollectionManager(d2)
	defer func() {
		if err := m2.Close(); err != nil {
			t.Errorf("reopened manager.Close: %v", err)
		}
		if err := cleanup2(); err != nil {
			t.Errorf("reopened cleanup: %v", err)
		}
	}()
	col2, err := m2.OpenCollection("repo_chunks")
	if err != nil {
		t.Fatalf("reopen OpenCollection: %v", err)
	}
	searchAfter, err := col2.SearchText(TextSearchOptions{IndexName: "lexical", Query: "anchor_common_00", TopK: 20})
	if err != nil {
		t.Fatalf("SearchText after reopen: %v", err)
	}
	if len(searchAfter.Results) != len(searchBefore.Results) {
		t.Fatalf("SearchText result count mismatch across reopen: before=%d after=%d", len(searchBefore.Results), len(searchAfter.Results))
	}
	phraseAfter, err := col2.SearchText(TextSearchOptions{IndexName: "lexical", Phrase: &TextSearchPhraseQuery{Query: "phrase_alpha phrase_beta"}, TopK: 20})
	if err != nil {
		t.Fatalf("PhraseSearch after reopen: %v", err)
	}
	if len(phraseAfter.Results) != len(phraseBefore.Results) {
		t.Fatalf("PhraseSearch result count mismatch across reopen: before=%d after=%d", len(phraseBefore.Results), len(phraseAfter.Results))
	}
}

func TestBufferedIndexedOverlayRoots_VacuumPreservesTombstones(t *testing.T) {
	run := func(t *testing.T, mode string) {
		ctx := context.Background()
		dir := t.TempDir()
		openDB := func() (*backenddb.DB, *CollectionManager, *Collection) {
			d, err := backenddb.Open(backenddb.Options{
				Dir:                        dir,
				IndexOuterLeavesInValueLog: false,
				DisableBackgroundPrune:     true,
			})
			if err != nil {
				t.Fatalf("open db: %v", err)
			}
			mgr := NewCollectionManager(d)
			col, err := mgr.OpenCollection("users")
			if err != nil {
				_ = mgr.Close()
				_ = d.Close()
				t.Fatalf("open collection: %v", err)
			}
			return d, mgr, col
		}

		d, err := backenddb.Open(backenddb.Options{
			Dir:                        dir,
			IndexOuterLeavesInValueLog: false,
			DisableBackgroundPrune:     true,
		})
		if err != nil {
			t.Fatalf("open db: %v", err)
		}
		mgr := NewCollectionManager(d)
		if _, err := mgr.CreateCollection(&CollectionMeta{
			Name: "users",
			Options: CollectionOptions{
				BufferedIndexedOverlayRoots:      true,
				BufferedIndexedWriteMaxDocuments: 1024,
				BufferedIndexedWriteMaxRootRuns:  1024,
				DisableBufferedIndexedAsyncFlush: true,
			},
			Indexes: []IndexDefinition{
				{Name: "city", Field: "city", ValueType: IndexValueString},
			},
		}); err != nil {
			_ = mgr.Close()
			_ = d.Close()
			t.Fatalf("create collection: %v", err)
		}
		col, err := mgr.OpenCollection("users")
		if err != nil {
			_ = mgr.Close()
			_ = d.Close()
			t.Fatalf("open collection: %v", err)
		}

		if _, err := col.Insert([]byte("u1"), []byte(`{"city":"hnl"}`)); err != nil {
			t.Fatalf("insert u1: %v", err)
		}
		if _, err := col.Insert([]byte("u2"), []byte(`{"city":"sea"}`)); err != nil {
			t.Fatalf("insert u2: %v", err)
		}
		if err := col.Flush(); err != nil {
			t.Fatalf("flush base docs: %v", err)
		}
		if _, err := col.CompactRootOverlays(ctx); err != nil {
			t.Fatalf("compact base overlay: %v", err)
		}

		// Delete u1 and flush into a delta overlay root whose tombstones mask
		// u1 (primary) and city=hnl (secondary index) in the base root.
		deleted, err := col.DeleteDocument([]byte("u1"))
		if err != nil {
			t.Fatalf("delete u1: %v", err)
		}
		if !deleted {
			t.Fatal("delete u1: deleted=false want true")
		}
		if err := col.Flush(); err != nil {
			t.Fatalf("flush delete overlay: %v", err)
		}

		assertDeleted := func(stage string, c *Collection) {
			t.Helper()
			if got, err := c.Get([]byte("u1")); err != nil {
				t.Fatalf("%s: Get(u1): %v", stage, err)
			} else if got != nil {
				t.Fatalf("%s: Get(u1)=%q resurrected deleted doc, want nil", stage, got)
			}
			hnlIDs, err := c.FindByIndex("city", "hnl")
			if err != nil {
				t.Fatalf("%s: FindByIndex(city, hnl): %v", stage, err)
			}
			if len(hnlIDs) != 0 {
				t.Fatalf("%s: FindByIndex(city, hnl)=%q resurrected deleted index entry, want empty", stage, hnlIDs)
			}
			if got, err := c.Get([]byte("u2")); err != nil || !bytes.Contains(got, []byte(`"sea"`)) {
				t.Fatalf("%s: Get(u2)=%q err=%v want sea doc", stage, got, err)
			}
			seaIDs, err := c.FindByIndex("city", "sea")
			if err != nil {
				t.Fatalf("%s: FindByIndex(city, sea): %v", stage, err)
			}
			if len(seaIDs) != 1 || !bytes.Equal(seaIDs[0], []byte("u2")) {
				t.Fatalf("%s: FindByIndex(city, sea)=%q want [u2]", stage, seaIDs)
			}
		}

		assertDeleted("before-vacuum", col)

		switch mode {
		case "online":
			if runtime.GOOS == "windows" {
				_ = mgr.Close()
				_ = d.Close()
				t.Skip("online vacuum unsupported on windows")
			}
			if err := d.VacuumIndexOnline(ctx); err != nil {
				_ = mgr.Close()
				_ = d.Close()
				t.Fatalf("VacuumIndexOnline: %v", err)
			}
			assertDeleted("after-online-vacuum", col)
			if err := mgr.Close(); err != nil {
				t.Fatalf("mgr.Close: %v", err)
			}
			if err := d.Close(); err != nil {
				t.Fatalf("d.Close: %v", err)
			}
		case "offline":
			if err := mgr.Close(); err != nil {
				t.Fatalf("mgr.Close: %v", err)
			}
			if err := d.Close(); err != nil {
				t.Fatalf("d.Close: %v", err)
			}
			if err := backenddb.VacuumIndexOffline(backenddb.Options{
				Dir:                        dir,
				IndexOuterLeavesInValueLog: false,
			}); err != nil {
				t.Fatalf("VacuumIndexOffline: %v", err)
			}
		}

		d2, mgr2, col2 := openDB()
		defer func() {
			if err := mgr2.Close(); err != nil {
				t.Errorf("reopened mgr.Close: %v", err)
			}
			if err := d2.Close(); err != nil {
				t.Errorf("reopened d.Close: %v", err)
			}
		}()
		assertDeleted("after-"+mode+"-reopen", col2)
	}

	t.Run("online", func(t *testing.T) { run(t, "online") })
	t.Run("offline", func(t *testing.T) { run(t, "offline") })
}
