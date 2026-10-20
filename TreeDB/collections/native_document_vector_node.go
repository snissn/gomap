package collections

import "fmt"

// Marked v5 uses the original node fields but encodes its arbitrary byte ID
// explicitly as base64. Legacy v3/v4 string records and their codec are unchanged.
// Both ID forms at once, or a legacy-only v5 record, are rejected rather than
// silently normalizing a lossy pre-alpha record into a certified graph.
type nativeDocumentPersistNode struct {
	vectorIndexPersistNode
	DocumentIDBytes []byte `json:"document_id_bytes"`
}

func decodeNativeDocumentGraphNode(raw []byte, decode func([]byte, any) error) (vectorIndexPersistNode, error) {
	var disk nativeDocumentPersistNode
	if err := decode(raw, &disk); err != nil {
		return vectorIndexPersistNode{}, err
	}
	if disk.DocumentID != "" || len(disk.DocumentIDBytes) == 0 {
		return vectorIndexPersistNode{}, fmt.Errorf("collections: native graph requires lossless byte document ID")
	}
	disk.DocumentID = string(disk.DocumentIDBytes)
	return disk.vectorIndexPersistNode, nil
}
