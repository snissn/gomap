package valuelog

// Test helper: keep file IDs consistent with canonical lane-tagged filenames.
func mustFileID(lane, seq uint32) uint32 {
	id, err := EncodeFileID(lane, seq)
	if err != nil {
		panic(err)
	}
	return id
}
