package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"

	"github.com/snissn/gomap/TreeDB/collections"
)

const (
	localHNSWM18EdgeCheckpointSchemaV1 = "treedb_local_hnsw_m18_edge_checkpoint_v1"
	localHNSWM18EdgeOriginsMagicV1     = "M18ORG01"
	localHNSWM18EdgeOriginsMaxBytesV1  = int64(512 << 20)
	localHNSWM18EdgeOriginsMaxEdgesV1  = uint64(20_000_000)
)

type localHNSWM18EdgeCheckpointV1 struct {
	Schema               string                                         `json:"schema"`
	HeadSHA              string                                         `json:"head_sha"`
	Manifest             string                                         `json:"manifest_integrity_digest"`
	DescriptorSHA256     string                                         `json:"descriptor_sha256"`
	Variant              string                                         `json:"variant"`
	CloneDirectory       string                                         `json:"clone_directory"`
	Origins              localHNSWAttributionFileInputV1                `json:"final_origins"`
	Build                localHNSWAttributionBuildEvidenceV1            `json:"build"`
	PackAssets           []collections.VectorPartitionAssetV1           `json:"pack_assets"`
	Construction         localHNSWAttributionConstructionTotalsV1       `json:"construction"`
	Neighborhood         localHNSWAttributionNeighborhoodOracleV1       `json:"neighborhood"`
	SelectedDiagnostics  []collections.VectorPartitionPackDiagnosticsV1 `json:"selected_pack_diagnostics"`
	SelectedConstruction []localHNSWM18EdgeDiagnosisPackConstructionV1  `json:"selected_pack_construction"`
	SelectedNeighborhood []localHNSWM18EdgeDiagnosisPackNeighborhoodV1  `json:"selected_pack_neighborhood"`
}

func localHNSWM18EdgeOriginCodeV1(origin string) (byte, bool) {
	switch origin {
	case "diversity_selected":
		return 1, true
	case "nearest_backfill":
		return 2, true
	case "reciprocal_add":
		return 3, true
	case "reciprocity_repair":
		return 4, true
	case "overlay_rewrite":
		return 5, true
	default:
		return 0, false
	}
}

func localHNSWM18EdgeOriginFromCodeV1(code byte) (string, bool) {
	for _, origin := range localHNSWAttributionConstructionOriginOrderV1 {
		if got, ok := localHNSWM18EdgeOriginCodeV1(origin); ok && got == code {
			return origin, true
		}
	}
	return "", false
}

func localHNSWM18EdgeOriginsWriteV1(path string, origins []map[localHNSWAttributionFinalEdgeKeyV1]string) (err error) {
	if len(origins) != 40 {
		return errors.New("invalid M18 checkpoint origin partitions")
	}
	temporary := path + ".tmp"
	if _, statErr := os.Lstat(path); !errors.Is(statErr, os.ErrNotExist) {
		return errors.New("M18 checkpoint origins exist")
	}
	f, err := os.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			err = errors.Join(err, f.Close())
		}
		if err != nil {
			_ = os.Remove(temporary)
		}
	}()
	w := bufio.NewWriterSize(f, 1<<20)
	if _, err = w.WriteString(localHNSWM18EdgeOriginsMagicV1); err != nil {
		return err
	}
	var header [8]byte
	binary.LittleEndian.PutUint32(header[:4], uint32(len(origins)))
	if _, err = w.Write(header[:4]); err != nil {
		return err
	}
	var total uint64
	for _, values := range origins {
		keys := make([]localHNSWAttributionFinalEdgeKeyV1, 0, len(values))
		for key, origin := range values {
			if key.From < 0 || key.To < 0 || key.Layer < 0 || key.From > math.MaxInt32 || key.To > math.MaxInt32 || key.Layer > math.MaxInt32 {
				return errors.New("invalid M18 checkpoint origin key")
			}
			if _, ok := localHNSWM18EdgeOriginCodeV1(origin); !ok {
				return errors.New("invalid M18 checkpoint origin")
			}
			keys = append(keys, key)
		}
		slices.SortFunc(keys, func(a, b localHNSWAttributionFinalEdgeKeyV1) int {
			if a.Layer != b.Layer {
				return a.Layer - b.Layer
			}
			if a.From != b.From {
				return a.From - b.From
			}
			return a.To - b.To
		})
		if len(keys) == 0 || math.MaxUint64-total < uint64(len(keys)) {
			return errors.New("invalid M18 checkpoint origin count")
		}
		total += uint64(len(keys))
		if total > localHNSWM18EdgeOriginsMaxEdgesV1 {
			return errors.New("oversized M18 checkpoint origins")
		}
		binary.LittleEndian.PutUint64(header[:], uint64(len(keys)))
		if _, err = w.Write(header[:]); err != nil {
			return err
		}
		var record [13]byte
		for _, key := range keys {
			binary.LittleEndian.PutUint32(record[0:4], uint32(key.Layer))
			binary.LittleEndian.PutUint32(record[4:8], uint32(key.From))
			binary.LittleEndian.PutUint32(record[8:12], uint32(key.To))
			record[12], _ = localHNSWM18EdgeOriginCodeV1(values[key])
			if _, err = w.Write(record[:]); err != nil {
				return err
			}
		}
	}
	if err = w.Flush(); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	closed = true
	if err = os.Rename(temporary, path); err != nil {
		return err
	}
	return nil
}

func localHNSWM18EdgeOriginsReadV1(input localHNSWAttributionFileInputV1) ([]map[localHNSWAttributionFinalEdgeKeyV1]string, error) {
	if !localHNSWAttributionSHA256V1(input.SHA256) {
		return nil, errors.New("invalid M18 checkpoint origin digest")
	}
	digest, err := localHNSWAttributionRegularFileSHA256V1(input.Path, localHNSWM18EdgeOriginsMaxBytesV1)
	if err != nil || digest != input.SHA256 {
		return nil, errors.New("M18 checkpoint origin digest mismatch")
	}
	f, err := os.Open(input.Path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 1<<20)
	magic := make([]byte, len(localHNSWM18EdgeOriginsMagicV1))
	if _, err := io.ReadFull(r, magic); err != nil || string(magic) != localHNSWM18EdgeOriginsMagicV1 {
		return nil, errors.New("invalid M18 checkpoint origin header")
	}
	var raw [13]byte
	if _, err := io.ReadFull(r, raw[:4]); err != nil || binary.LittleEndian.Uint32(raw[:4]) != 40 {
		return nil, errors.New("invalid M18 checkpoint origin partitions")
	}
	out := make([]map[localHNSWAttributionFinalEdgeKeyV1]string, 40)
	var total uint64
	for partition := range out {
		if _, err := io.ReadFull(r, raw[:8]); err != nil {
			return nil, err
		}
		count := binary.LittleEndian.Uint64(raw[:8])
		if count == 0 || count > localHNSWM18EdgeOriginsMaxEdgesV1 || math.MaxUint64-total < count || total+count > localHNSWM18EdgeOriginsMaxEdgesV1 {
			return nil, errors.New("invalid M18 checkpoint origin count")
		}
		total += count
		out[partition] = make(map[localHNSWAttributionFinalEdgeKeyV1]string, int(count))
		var previous localHNSWAttributionFinalEdgeKeyV1
		for i := uint64(0); i < count; i++ {
			if _, err := io.ReadFull(r, raw[:]); err != nil {
				return nil, err
			}
			key := localHNSWAttributionFinalEdgeKeyV1{Layer: int(binary.LittleEndian.Uint32(raw[0:4])), From: int(binary.LittleEndian.Uint32(raw[4:8])), To: int(binary.LittleEndian.Uint32(raw[8:12]))}
			origin, ok := localHNSWM18EdgeOriginFromCodeV1(raw[12])
			if !ok || i > 0 && (key.Layer < previous.Layer || key.Layer == previous.Layer && (key.From < previous.From || key.From == previous.From && key.To <= previous.To)) {
				return nil, errors.New("noncanonical M18 checkpoint origins")
			}
			out[partition][key] = origin
			previous = key
		}
	}
	if _, err := r.ReadByte(); !errors.Is(err, io.EOF) {
		return nil, errors.New("trailing M18 checkpoint origins")
	}
	return out, nil
}

func localHNSWM18EdgeCheckpointValidateV1(value localHNSWM18EdgeCheckpointV1, source *m8ProductionMultiGroupAssetsV1, headSHA, descriptorSHA, tempRoot string) error {
	if source == nil || value.Schema != localHNSWM18EdgeCheckpointSchemaV1 || value.HeadSHA != headSHA || value.Manifest != source.manifest.IntegrityDigest || value.DescriptorSHA256 != descriptorSHA || value.Variant != string(collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256V1) || value.Build.Schema != localHNSWAttributionBuildSchemaV1 || value.Build.Variant != value.Variant || value.Build.Partitions != 40 || len(value.PackAssets) != 40 || value.Construction.FinalSurvivors == 0 || value.Construction.OriginOrder != localHNSWAttributionConstructionOriginOrderV1 || value.Neighborhood.Schema != localHNSWAttributionNeighborhoodOracleSchemaV1 || len(value.SelectedDiagnostics) != 5 || len(value.SelectedConstruction) != 5 || len(value.SelectedNeighborhood) != 5 {
		return errors.New("invalid M18 prepared checkpoint")
	}
	clone, err := m8CanonicalPathV1(value.CloneDirectory)
	if err != nil || clone != value.CloneDirectory {
		return errors.New("invalid M18 checkpoint clone")
	}
	relative, err := filepath.Rel(tempRoot, clone)
	if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || filepath.IsAbs(relative) {
		return errors.New("M18 checkpoint clone outside temporary root")
	}
	for i, asset := range value.PackAssets {
		if asset.PartitionID != uint32(i) || asset.ID == "" || asset.Checksum == "" || asset.Bytes == 0 {
			return errors.New("invalid M18 checkpoint pack assets")
		}
	}
	for i, partition := range localHNSWM18EdgeDiagnosisPacksV1 {
		if value.SelectedConstruction[i].Partition != partition || value.SelectedNeighborhood[i].Partition != partition {
			return errors.New("invalid M18 checkpoint selected packs")
		}
	}
	return nil
}

func localHNSWM18EdgeCheckpointWriteV1(path, headSHA, descriptorSHA, tempRoot string, source *m8ProductionMultiGroupAssetsV1, h *localHNSWVariantHarnessV1, build localHNSWAttributionBuildEvidenceV1, construction localHNSWAttributionConstructionTotalsV1, selectedConstruction []localHNSWM18EdgeDiagnosisPackConstructionV1, diagnostics []collections.VectorPartitionPackDiagnosticsV1, neighborhood localHNSWAttributionNeighborhoodOracleV1, selectedNeighborhood []localHNSWM18EdgeDiagnosisPackNeighborhoodV1) (localHNSWM18EdgeCheckpointV1, error) {
	if h == nil || h.assets == nil || h.assets.dir == "" || len(diagnostics) != 40 {
		return localHNSWM18EdgeCheckpointV1{}, errors.New("invalid M18 checkpoint harness")
	}
	originsPath := path + ".origins.bin"
	if err := localHNSWM18EdgeOriginsWriteV1(originsPath, h.finalOrigins); err != nil {
		return localHNSWM18EdgeCheckpointV1{}, err
	}
	keepCheckpoint := false
	defer func() {
		if !keepCheckpoint {
			_ = os.Remove(originsPath)
			_ = os.Remove(path)
		}
	}()
	originsSHA, err := localHNSWAttributionRegularFileSHA256V1(originsPath, localHNSWM18EdgeOriginsMaxBytesV1)
	if err != nil {
		return localHNSWM18EdgeCheckpointV1{}, err
	}
	selectedDiagnostics := make([]collections.VectorPartitionPackDiagnosticsV1, len(localHNSWM18EdgeDiagnosisPacksV1))
	for i, partition := range localHNSWM18EdgeDiagnosisPacksV1 {
		selectedDiagnostics[i] = diagnostics[partition]
	}
	value := localHNSWM18EdgeCheckpointV1{Schema: localHNSWM18EdgeCheckpointSchemaV1, HeadSHA: headSHA, Manifest: source.manifest.IntegrityDigest, DescriptorSHA256: descriptorSHA, Variant: string(collections.VectorPartitionLocalGraphVariantAuxiliaryNavigationM18EfConstruction256V1), CloneDirectory: h.assets.dir, Origins: localHNSWAttributionFileInputV1{Path: originsPath, SHA256: originsSHA}, Build: build, PackAssets: append([]collections.VectorPartitionAssetV1(nil), h.packAssets...), Construction: construction, Neighborhood: neighborhood, SelectedDiagnostics: selectedDiagnostics, SelectedConstruction: append([]localHNSWM18EdgeDiagnosisPackConstructionV1(nil), selectedConstruction...), SelectedNeighborhood: append([]localHNSWM18EdgeDiagnosisPackNeighborhoodV1(nil), selectedNeighborhood...)}
	if err := localHNSWM18EdgeCheckpointValidateV1(value, source, headSHA, descriptorSHA, tempRoot); err != nil {
		return localHNSWM18EdgeCheckpointV1{}, err
	}
	if err := writeVectorPartitionSystemJSONExclusiveV1(path, value); err != nil {
		return localHNSWM18EdgeCheckpointV1{}, err
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return localHNSWM18EdgeCheckpointV1{}, err
	}
	var reread localHNSWM18EdgeCheckpointV1
	if json.Unmarshal(raw, &reread) != nil || !reflect.DeepEqual(reread, value) {
		return localHNSWM18EdgeCheckpointV1{}, errors.New("M18 checkpoint reread")
	}
	keepCheckpoint = true
	return value, nil
}

func localHNSWM18EdgeCheckpointOpenV1(path, headSHA, descriptorSHA, tempRoot string, source *m8ProductionMultiGroupAssetsV1) (_ localHNSWM18EdgeCheckpointV1, _ *localHNSWVariantHarnessV1, err error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return localHNSWM18EdgeCheckpointV1{}, nil, err
	}
	var value localHNSWM18EdgeCheckpointV1
	if json.Unmarshal(raw, &value) != nil || localHNSWM18EdgeCheckpointValidateV1(value, source, headSHA, descriptorSHA, tempRoot) != nil {
		return localHNSWM18EdgeCheckpointV1{}, nil, errors.New("invalid M18 checkpoint")
	}
	origins, err := localHNSWM18EdgeOriginsReadV1(value.Origins)
	if err != nil {
		return localHNSWM18EdgeCheckpointV1{}, nil, err
	}
	owned, err := openM8ProductionExistingAssetSetModeV1(value.CloneDirectory, false)
	if err != nil {
		return localHNSWM18EdgeCheckpointV1{}, nil, err
	}
	h := &localHNSWVariantHarnessV1{assets: owned, packAssets: append([]collections.VectorPartitionAssetV1(nil), value.PackAssets...), searchers: make([]*collections.VectorPartitionLocalSearcherV1, len(value.PackAssets)), documentIDs: make([][]string, len(value.PackAssets)), finalOrigins: origins}
	defer func() {
		if err != nil {
			err = errors.Join(err, h.Close())
		}
	}()
	if owned.manifest.IntegrityDigest != source.manifest.IntegrityDigest || owned.manifest.SourceChecksum != source.manifest.SourceChecksum || owned.manifest.Generation != source.manifest.Generation {
		return localHNSWM18EdgeCheckpointV1{}, nil, errors.New("M18 checkpoint clone identity")
	}
	for partition, asset := range h.packAssets {
		h.searchers[partition], err = owned.collection.OpenVectorPartitionLocalSearcherForOfflineAssetWithContextV1(context.Background(), source.manifest.IndexName, source.manifest, asset)
		if err != nil {
			return localHNSWM18EdgeCheckpointV1{}, nil, err
		}
		h.documentIDs[partition], err = h.searchers[partition].PackDocumentIDsForOfflineTraceV1()
		if err != nil || len(h.documentIDs[partition]) == 0 {
			return localHNSWM18EdgeCheckpointV1{}, nil, errors.New("M18 checkpoint document IDs")
		}
	}
	diagnostics, err := localHNSWAttributionPackDiagnosticsV1(h.searchers)
	if err != nil {
		return localHNSWM18EdgeCheckpointV1{}, nil, err
	}
	for i, partition := range localHNSWM18EdgeDiagnosisPacksV1 {
		if !reflect.DeepEqual(diagnostics[partition], value.SelectedDiagnostics[i]) {
			return localHNSWM18EdgeCheckpointV1{}, nil, fmt.Errorf("M18 checkpoint pack %d diagnostics mismatch", partition)
		}
	}
	return value, h, nil
}
