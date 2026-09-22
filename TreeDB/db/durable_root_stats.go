package db

import "strconv"

// appendDurableRootStats renders the bounded durable-root selection that
// treemap stats and verify -report expose. Publication owns durableRoot under
// durablePublishMu; copy the scalar record while holding that lock and format
// it afterward so reporting never observes a mixed slot/record tuple.
func (db *DB) appendDurableRootStats(stats map[string]string) {
	if db == nil || stats == nil {
		return
	}
	db.durablePublishMu.Lock()
	selected := db.durableRoot
	db.durablePublishMu.Unlock()

	record := selected.record
	stats["treedb.durable_root.format_version"] = "1"
	stats["treedb.durable_root.selected_slot"] = strconv.FormatUint(selected.slot, 10)
	stats["treedb.durable_root.commit_seq"] = strconv.FormatUint(record.CommitSeq, 10)
	stats["treedb.durable_root.durable_seq"] = strconv.FormatUint(record.DurableSeq, 10)
	stats["treedb.durable_root.record_page"] = strconv.FormatUint(selected.meta.RootRecordPageID, 10)
	stats["treedb.durable_root.user_root_page"] = strconv.FormatUint(record.UserRootPageID, 10)
	stats["treedb.durable_root.system_root_page"] = strconv.FormatUint(record.SystemRootPageID, 10)
	stats["treedb.durable_root.total_pages"] = strconv.FormatUint(record.TotalPages, 10)
	stats["treedb.durable_root.freelist.header_page"] = strconv.FormatUint(record.Freelist.HeaderPageID, 10)
	stats["treedb.durable_root.freelist.generation"] = strconv.FormatUint(record.Freelist.GenerationID, 10)
	stats["treedb.durable_root.freelist.high_water"] = strconv.FormatUint(record.Freelist.HighWater, 10)
	stats["treedb.durable_root.freelist.free_count"] = strconv.FormatUint(record.FreelistFreeCount, 10)
	stats["treedb.durable_root.freelist.retired_count"] = strconv.FormatUint(record.FreelistRetiredCount, 10)
	stats["treedb.durable_root.manifest.first_page"] = strconv.FormatUint(record.Manifest.FirstPageID, 10)
	stats["treedb.durable_root.manifest.bytes"] = strconv.FormatUint(record.Manifest.ByteLength, 10)
	stats["treedb.durable_root.manifest.entries"] = strconv.FormatUint(uint64(record.Manifest.EntryCount), 10)
	stats["treedb.durable_root.manifest.pages"] = strconv.FormatUint(uint64(record.Manifest.PageCount), 10)
	stats["treedb.durable_root.manifest_build.count"] = strconv.FormatUint(db.durableRootManifestBuildCount.Load(), 10)
	stats["treedb.durable_root.manifest_build.nanos"] = strconv.FormatUint(db.durableRootManifestBuildNs.Load(), 10)
	stats["treedb.durable_root.manifest_build.entries_visited"] = strconv.FormatUint(db.durableRootManifestEntriesSeen.Load(), 10)
	stats["treedb.durable_root.manifest_build.entries_encoded"] = strconv.FormatUint(db.durableRootManifestEntriesEncoded.Load(), 10)
	stats["treedb.durable_root.manifest_build.bytes_encoded"] = strconv.FormatUint(db.durableRootManifestBytesEncoded.Load(), 10)
	stats["treedb.durable_root.parent.record_page"] = strconv.FormatUint(record.ParentRecordPageID, 10)
	stats["treedb.durable_root.parent.commit_seq"] = strconv.FormatUint(record.ParentCommitSeq, 10)
	stats["treedb.durable_root.slot0.commit_seq"] = strconv.FormatUint(selected.slotCommit[0], 10)
	stats["treedb.durable_root.slot1.commit_seq"] = strconv.FormatUint(selected.slotCommit[1], 10)
}
