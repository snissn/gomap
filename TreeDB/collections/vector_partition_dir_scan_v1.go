package collections

import (
	"errors"
	"fmt"
	"io"
	"os"
)

const vectorPartitionDirReadBatchV1 = 64

// readVectorPartitionDirEntriesBoundedV1 reads the whole retained namespace in
// bounded batches and rejects the first entry beyond the global store cap.
// The cap applies to every directory entry, not only names relevant to one
// collection/index identity.
func readVectorPartitionDirEntriesBoundedV1(dir *os.File) ([]os.DirEntry, error) {
	if dir == nil {
		return nil, fmt.Errorf("%w: nil vector partition directory", ErrVectorPartitionManifestInvalid)
	}
	if _, err := dir.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	entries := make([]os.DirEntry, 0, vectorPartitionDirReadBatchV1)
	for {
		batch, err := dir.ReadDir(vectorPartitionDirReadBatchV1)
		if len(batch) > vectorPartitionStoreMaxEntriesV1-len(entries) {
			return nil, fmt.Errorf("%w: vector partition directory entry cap", ErrVectorPartitionManifestInvalid)
		}
		entries = append(entries, batch...)
		if errors.Is(err, io.EOF) {
			return entries, nil
		}
		if err != nil {
			return nil, err
		}
		if len(batch) == 0 {
			return entries, nil
		}
	}
}
