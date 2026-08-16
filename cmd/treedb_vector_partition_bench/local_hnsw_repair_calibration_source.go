package main

import (
	"errors"

	"github.com/snissn/gomap/TreeDB/collections"
)

// localHNSWRepairCalibrationBindDescriptorV1 verifies the immutable retained
// source identity for the offline V2-overlay-versus-V3-repair experiment. It
// intentionally does not apply current production asset readiness rules to
// the historical V2 before pack: the rebuilt overlay is checked against those
// retained pack bytes before queries begin.
func localHNSWRepairCalibrationBindDescriptorV1(h *m8ProductionMultiGroupAssetsV1, fixture fixtureManifest) error {
	if h == nil || h.collection == nil || h.router == nil {
		return errors.New("retained local HNSW repair assets are not open")
	}
	descriptor, err := m3ReadVariantDescriptorV1(h.dir)
	if err != nil {
		return err
	}
	if err := m3DescriptorMatchesManifestV1(descriptor, fixture, h.status.Manifest, h.status.ModelDigest, h.status.Config); err != nil {
		return err
	}
	var indexDefinitionDigest string
	for _, index := range h.collection.MetaView().VectorIndexes {
		if index.Name == partitionHNSWIndex {
			indexDefinitionDigest = collections.VectorIndexDefinitionDigestV1(index)
			break
		}
	}
	// The source index definition and the materialized partition-local graph are
	// intentionally separate identities. The promoted default may use M18 while
	// the canonical source index remains M16.
	if indexDefinitionDigest != descriptor.IndexDefinitionDigest {
		return errors.New("retained local HNSW repair descriptor source definition mismatch")
	}
	h.descriptor = &descriptor
	return nil
}
