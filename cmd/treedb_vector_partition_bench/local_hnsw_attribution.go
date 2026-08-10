package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strconv"
	"strings"
)

type localHNSWQuerySplitV1 struct {
	Schema              string `json:"schema"`
	DatasetChecksum     string `json:"dataset_checksum"`
	TruthArtifactSHA256 string `json:"truth_artifact_sha256"`
	Selection           string `json:"selection"`
	Ordinals            []int  `json:"ordinals"`
}

const localHNSWQuerySplitSelectionV1 = "sha256(4105-local-hnsw-calibration-v1:<query-ordinal>)[0] < 205"
const localHNSWQuerySplitMaxBytesV1 = 1 << 20

func loadLocalHNSWQuerySplitV1(path string) (localHNSWQuerySplitV1, string, error) {
	f, err := os.Open(path)
	if err != nil {
		return localHNSWQuerySplitV1{}, "", err
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, localHNSWQuerySplitMaxBytesV1+1))
	if err != nil {
		return localHNSWQuerySplitV1{}, "", err
	}
	if len(raw) > localHNSWQuerySplitMaxBytesV1 {
		return localHNSWQuerySplitV1{}, "", errors.New("query split exceeds cap")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	var split localHNSWQuerySplitV1
	if err = d.Decode(&split); err != nil {
		return split, "", err
	}
	if d.Decode(&struct{}{}) != io.EOF {
		return split, "", errors.New("query split trailing JSON")
	}
	sum := sha256.Sum256(raw)
	return split, hex.EncodeToString(sum[:]), nil
}

func localHNSWCalibrationOrdinalV1(ordinal int) bool {
	sum := sha256.Sum256([]byte("4105-local-hnsw-calibration-v1:" + strconv.Itoa(ordinal)))
	return sum[0] < 205
}

func validateLocalHNSWQuerySplitPairV1(calibration, holdout localHNSWQuerySplitV1, fixture fixtureManifest, trustedTruth string) error {
	if fixture.Queries <= 0 || len(trustedTruth) != 64 || strings.ToLower(trustedTruth) != trustedTruth {
		return errors.New("query split trusted truth")
	}
	if _, err := hex.DecodeString(trustedTruth); err != nil {
		return errors.New("query split trusted truth")
	}
	if calibration.Schema != "vector_partition_4105_query_split_v1" ||
		holdout.Schema != calibration.Schema ||
		calibration.Selection != localHNSWQuerySplitSelectionV1 ||
		holdout.Selection != localHNSWQuerySplitSelectionV1 ||
		calibration.DatasetChecksum != fixture.Checksum ||
		holdout.DatasetChecksum != fixture.Checksum ||
		calibration.TruthArtifactSHA256 != trustedTruth ||
		holdout.TruthArtifactSHA256 != trustedTruth {
		return errors.New("query split identity")
	}
	seen := make([]bool, fixture.Queries)
	for kind, split := range []localHNSWQuerySplitV1{calibration, holdout} {
		for i, x := range split.Ordinals {
			if x < 0 || x >= fixture.Queries || seen[x] || i > 0 && split.Ordinals[i-1] >= x || localHNSWCalibrationOrdinalV1(x) != (kind == 0) {
				return errors.New("query split ordinals")
			}
			seen[x] = true
		}
	}
	for _, ok := range seen {
		if !ok {
			return errors.New("query split coverage")
		}
	}
	return nil
}
