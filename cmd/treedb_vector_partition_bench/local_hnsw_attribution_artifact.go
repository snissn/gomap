package main

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"io"
	"os"
)

const localHNSWAttributionSidecarSchemaV1 = "treedb_local_hnsw_attribution_sidecar_v1"

type localHNSWAttributionArtifactV1 struct {
	Schema  string `json:"schema"`
	Path    string `json:"path"`
	SHA256  string `json:"sha256"`
	Bytes   int64  `json:"bytes"`
	Records int    `json:"records"`
}

func localHNSWAttributionWriteGzipJSONLV1(path string, write func(*json.Encoder) (int, error)) (localHNSWAttributionArtifactV1, error) {
	var out localHNSWAttributionArtifactV1
	if path == "" || write == nil {
		return out, errors.New("invalid local HNSW attribution sidecar")
	}
	records := 0
	digest, linked, err := m8PublishTruthCacheWithDirectorySyncV1(path, "", func(dst io.Writer) error {
		compressed := gzip.NewWriter(dst)
		var writeErr error
		records, writeErr = write(json.NewEncoder(compressed))
		if writeErr == nil && records < 1 {
			writeErr = errors.New("empty local HNSW attribution sidecar")
		}
		return errors.Join(writeErr, compressed.Close())
	}, m8SyncDirectoryV1)
	if err != nil || !linked || records < 1 || !localHNSWAttributionSHA256V1(digest) {
		return out, errors.Join(errors.New("write local HNSW attribution sidecar"), err)
	}
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Size() < 1 {
		return out, errors.New("invalid local HNSW attribution sidecar output")
	}
	return localHNSWAttributionArtifactV1{Schema: localHNSWAttributionSidecarSchemaV1, Path: path, SHA256: digest, Bytes: info.Size(), Records: records}, nil
}
