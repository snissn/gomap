package nativewire

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
	"github.com/snissn/gomap/TreeDB/internal/raftcluster"
	"github.com/snissn/gomap/TreeDB/internal/raftplacement"
	public "github.com/snissn/gomap/TreeDB/vectorpartition"
)

// FixedPeerVectorBootstrapV1 retains real partial results on failure. It is
// a fresh-cluster operator fixture, not a general loader or recovery receipt.
type FixedPeerVectorBootstrapV1 struct {
	Stage        string
	Catalog      raftplacement.CatalogMetaStatusV1
	Create, Seed raftcluster.SubmitResultV1
	Prepare      collections.VectorPartitionPrepareCompletionV1
}
type FixedPeerVectorQualificationV1 struct {
	Prepare       collections.VectorPartitionPrepareCompletionV1
	Before, After public.SearchResponseV1
	Insert, Retry public.InsertResponseV1
	Readiness     []FixedPeerReadinessV1
}

func validateFixedPeerFixtureV1(config FixedPeerTCPConfigV1, requestID string) error {
	v := config.VectorInitialization
	if config.Credentials == nil || v == nil || len(config.Groups) != 1 || (len(config.Nodes) != 3 && len(config.Nodes) != 4) {
		return fmt.Errorf("fixture requires authenticated single-group RF3/RF4 initialization")
	}
	def := v.IndexDefinition
	if v.Generation != 1 || v.CatalogEpoch != 1 ||
		v.Collection != (raftplacement.CollectionRefV1{Database: "default", Catalog: "default", Collection: "docs"}) ||
		v.SourceGroupID == "" || v.SourceGroupID != config.Groups[0].ID ||
		v.MaxSourceRows < 3 || v.MaxSourceRows > 512 ||
		def.Name != "embedding_graph" || def.Field != "embedding" || def.Metric != collections.VectorMetricCosine ||
		def.Dimensions != 2 || def.M != 2 || def.EfConstruction != 8 || def.EfSearch != 8 ||
		def.Strategy != collections.VectorIndexStrategyColumnGraph ||
		def.Encoding != collections.VectorIndexEncodingFloat32 ||
		def.Representation != "" || def.SchemaGeneration != 0 || len(def.QuantizedIndexes) != 0 {
		return fmt.Errorf("fixture requires canonical generation1/epoch1 default.default.docs embedding_graph and MaxSourceRows3..512")
	}
	if requestID == "" || len(requestID) > 64 || strings.Trim(requestID, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-") != "" {
		return fmt.Errorf("request ID must contain 1..64 ASCII letters, digits, underscores or hyphens")
	}
	return nil
}
func fixturePollV1(ctx context.Context, check func() (bool, error)) error {
	if _, ok := ctx.Deadline(); !ok {
		return fmt.Errorf("bounded operation context requires a deadline")
	}
	timer := time.NewTicker(50 * time.Millisecond)
	defer timer.Stop()
	var last error
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("operation deadline/cancellation: %w (last observation: %v)", err, last)
		}
		ok, err := check()
		if ok && err == nil {
			return nil
		}
		last = err
		select {
		case <-ctx.Done():
			return fmt.Errorf("operation deadline/cancellation: %w (last observation: %v)", ctx.Err(), last)
		case <-timer.C:
		}
	}
}
func (c *FixedPeerTCPClientV1) fixturePrefixV1(ctx context.Context, index uint64) error {
	return fixturePollV1(ctx, func() (bool, error) {
		for _, node := range c.config.Nodes {
			state, err := c.Status(ctx, node.ID)
			if err != nil {
				return false, err
			}
			if len(state.Groups) != 1 || state.Groups[0].Applied.Index < index {
				return false, nil
			}
		}
		return true, nil
	})
}
func fixtureEntryV1(command iwire.CommandID, sections []iwire.Section) ([]byte, error) {
	sections = append([]iwire.Section{{ID: iwire.SectionCommandHeader, Bytes: iwire.AppendCommandHeader(nil, iwire.CommandHeader{ID: command, Version: 1})}}, sections...)
	validated, err := iwire.MustV1Registry().ValidateRequestSections(sections)
	if err != nil {
		return nil, err
	}
	return iwire.AppendDeterministicEntry(nil, validated)
}
func fixtureMetaV1(v *FixedPeerTCPVectorInitializationV1) (collections.CollectionMeta, error) {
	return normalizeClientCollectionMeta(collections.CollectionMeta{Name: v.Collection.Collection, Options: collections.CollectionOptions{
		DocumentFormat: collections.DocumentFormatJSON,
		ColumnStore: &collections.ColumnStoreConfig{Enabled: true, Columns: []collections.ColumnStoreColumn{
			{Name: "kind", Path: "kind", ValueType: collections.ColumnStoreValueString},
			{Name: "embedding", Path: "embedding", Owner: collections.TypedStorageOwnerColumnPart, ValueType: collections.ColumnStoreValueFloat32Vector, VectorDims: 2},
		}}}, VectorIndexes: []collections.VectorIndexDefinition{v.IndexDefinition}})
}

// InitializeVectorFixtureV1 publishes the exact immutable catalog, creates
// physical schema, and seeds three vectors through actual routed Raft commands.
// Mutations are never automatically retried: an ambiguous commit needs operator
// investigation. A successful prepare requires clean restart before serving.
func (c *FixedPeerTCPClientV1) InitializeVectorFixtureV1(ctx context.Context, requestID string) (report FixedPeerVectorBootstrapV1, err error) {
	if err = validateFixedPeerFixtureV1(c.config, requestID); err != nil {
		return
	}
	v := c.config.VectorInitialization
	report.Stage = "fresh-cluster-check"
	err = fixturePollV1(ctx, func() (bool, error) {
		if _, e := c.leader(ctx, c.config.Catalog); e != nil {
			return false, e
		}
		for _, node := range c.config.Nodes {
			s, e := c.Status(ctx, node.ID)
			if e != nil {
				return false, e
			}
			if s.Catalog.Epoch != 0 || s.RecoveryState != "new" || len(s.Groups) != 1 || s.Groups[0].Applied.HasApplied {
				return false, fmt.Errorf("initialize requires fresh empty catalog and collections; node %s is already initialized", node.ID)
			}
		}
		return true, nil
	})
	if err != nil {
		return
	}
	record, e := raftplacement.NewCatalogMetaRecordV1(v.CatalogEpoch, fixedPeerVectorInitializationCatalogV1(c.config))
	if e != nil {
		err = e
		return
	}
	raw, e := raftplacement.EncodeCatalogMetaCommandV1(raftplacement.CatalogMetaCommandV1{Record: record})
	if e != nil {
		err = e
		return
	}
	leader, e := c.leader(ctx, c.config.Catalog)
	if e != nil {
		err = e
		return
	}
	report.Stage = "catalog-publish"
	report.Catalog, err = c.PublishCatalog(ctx, leader, raw)
	if err != nil {
		return
	}
	err = fixturePollV1(ctx, func() (bool, error) {
		for _, node := range c.config.Nodes {
			s, e := c.Status(ctx, node.ID)
			if e != nil {
				return false, e
			}
			if s.Catalog.Epoch != record.Epoch || s.Catalog.Digest != record.Digest {
				return false, nil
			}
		}
		if _, e := c.leader(ctx, c.config.Groups[0]); e != nil {
			return false, e
		}
		return true, nil
	})
	if err != nil {
		return
	}
	submit := func(command iwire.CommandID, sections []iwire.Section) (raftcluster.SubmitResultV1, error) {
		entry, e := fixtureEntryV1(command, sections)
		if e != nil {
			return raftcluster.SubmitResultV1{}, e
		}
		request := ClusterRouteRequest{Database: v.Collection.Database, Catalog: v.Collection.Catalog, Collection: v.Collection.Collection, Shape: ClusterRouteShapeCollection}
		route, e := c.Route(ctx, c.config.NodeID, request)
		if e != nil {
			return raftcluster.SubmitResultV1{}, e
		}
		metadata := ClusterRequestMetadata{AckPolicy: iwire.AckRaftCommitted}
		ApplyClusterRouteMetadata(&metadata, request, route)
		result, e := c.Submit(ctx, c.config.NodeID, entry, metadata)
		if e == nil && (!result.CommittedApplied || !result.CommittedRecoverable || !result.Evidence.ProvesProductionConsensus()) {
			e = fmt.Errorf("missing real committed/apply evidence")
		}
		return result, e
	}
	meta, e := fixtureMetaV1(v)
	if e != nil {
		err = e
		return
	}
	encoded, e := encodeCollectionMeta(meta)
	if e != nil {
		err = e
		return
	}
	owner, e := c.leader(ctx, c.config.Groups[0])
	if e != nil {
		err = e
		return
	}
	state, e := c.Status(ctx, owner)
	if e != nil {
		err = e
		return
	}
	if len(state.Groups) != 1 {
		err = fmt.Errorf("missing source-group status")
		return
	}
	report.Stage = "create"
	report.Create, err = submit(iwire.CommandCreateCollection, []iwire.Section{
		{ID: iwire.SectionIdempotencyKey, Bytes: []byte(requestID + "/create")},
		{ID: iwire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, state.Groups[0].CatalogVersion)},
		{ID: iwire.SectionCollectionMeta, Bytes: encoded}, ackSection(AckRaftCommitted)})
	if err != nil {
		return
	}
	if err = c.fixturePrefixV1(ctx, report.Create.Evidence.Index); err != nil {
		return
	}
	owner, e = c.leader(ctx, c.config.Groups[0])
	if e != nil {
		err = e
		return
	}
	state, e = c.Status(ctx, owner)
	if e != nil {
		err = e
		return
	}
	if len(state.Groups) != 1 {
		err = fmt.Errorf("missing source-group status")
		return
	}
	report.Stage = "seed"
	report.Seed, err = submit(iwire.CommandInsertBatch, []iwire.Section{
		{ID: iwire.SectionIdempotencyKey, Bytes: []byte(requestID + "/seed")},
		{ID: iwire.SectionExpectedCatalogVersion, Bytes: binary.AppendUvarint(nil, state.Groups[0].CatalogVersion)},
		collectionNameRef(meta.Name), documentFormatSection(collections.DocumentFormatJSON),
		{ID: iwire.SectionDocumentIDs, Bytes: iwire.AppendByteVector(nil, []byte("seed-x"), []byte("seed-minus-x"), []byte("seed-minus-y"))},
		{ID: iwire.SectionDocuments, Bytes: iwire.AppendByteVector(nil, []byte(`{"embedding":[1,0],"kind":"seed"}`), []byte(`{"embedding":[-1,0],"kind":"seed"}`), []byte(`{"embedding":[0,-1],"kind":"seed"}`))}, ackSection(AckRaftCommitted)})
	if err != nil {
		return
	}
	if err = c.fixturePrefixV1(ctx, report.Seed.Evidence.Index); err != nil {
		return
	}
	report.Stage = "prepare"
	report.Prepare, err = c.PrepareVectorInitializationV1(ctx, c.config.NodeID, requestID)
	if err == nil && report.Prepare.Command.SourceRowCount != 3 {
		err = fmt.Errorf("fixture requires exactly3 prepared source rows; observed %d", report.Prepare.Command.SourceRowCount)
	}
	if err == nil {
		report.Stage = "prepared-restart-required"
	}
	return
}

// fixturePreparationV1 only reads existing authenticated durable completions.
// Missing observations may converge; completed mismatches refuse immediately.
// This proves the frozen preparation count, not current collection cardinality.
func (c *FixedPeerTCPClientV1) fixturePreparationV1(ctx context.Context, requestID string) (observed collections.VectorPartitionPrepareCompletionV1, err error) {
	if _, ok := ctx.Deadline(); !ok {
		return observed, fmt.Errorf("bounded operation context requires a deadline")
	}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return observed, fmt.Errorf("fixture preparation observation incomplete: %w", err)
		}
		var first *collections.VectorPartitionPrepareCompletionV1
		pending := false
		for _, peer := range c.config.Groups[0].Peers {
			reply, e := c.call(ctx, peer.ID, "vector-prepare-status", fixedPeerRequestV1{}, false)
			state := reply.VectorPreparation
			if e != nil || state == nil || state.Completion == nil {
				pending = true
				continue
			}
			completion := state.Completion
			observed = *completion
			v := completion.Command
			if v.ValidateV1() != nil || v.Operation != "prepare" || v.Term == 0 ||
				state.RequestID != requestID+"/prepare" || v.SourceRowCount != 3 ||
				state.Source != (collections.VectorPartitionSourceIdentityV1{Generation: v.SourceGeneration, Checksum: v.SourceChecksum, SchemaHash: v.SourceSchemaHash, RowCount: v.SourceRowCount}) {
				return observed, fmt.Errorf("fixture preparation requires matching request and exactly3 prepared source rows; observed request=%q rows=%d", state.RequestID, v.SourceRowCount)
			}
			if first == nil {
				first = completion
			} else if completion.Command != first.Command || completion.AssetSetDigest != first.AssetSetDigest {
				return observed, fmt.Errorf("fixture preparation completed voters disagree")
			}
		}
		if !pending && first != nil {
			return *first, nil
		}
		select {
		case <-ctx.Done():
			return observed, fmt.Errorf("fixture preparation observation incomplete: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

// QualifyVectorFixtureV1 runs after clean restart. It verifies exact known
// top-one cosine results, authentic fresh-write/retry receipts, all-voter
// applied-prefix visibility and readiness. Retry consensus progress can advance;
// it does not replace the retained original Insert response.
func (c *FixedPeerTCPClientV1) QualifyVectorFixtureV1(ctx context.Context, requestID string) (report FixedPeerVectorQualificationV1, err error) {
	if err = validateFixedPeerFixtureV1(c.config, requestID); err != nil {
		return
	}
	if _, ok := ctx.Deadline(); !ok {
		err = fmt.Errorf("bounded operation context requires a deadline")
		return
	}
	report.Prepare, err = c.fixturePreparationV1(ctx, requestID)
	if err != nil {
		return
	}
	v := c.config.VectorInitialization
	var client *Client
	err = fixturePollV1(ctx, func() (bool, error) {
		var e error
		client, e = DialContext(ctx, "tcp", v.PublicAddresses[c.config.NodeID])
		return e == nil, e
	})
	if err != nil {
		return
	}
	defer client.Close()
	generation := public.GenerationIDV1{Index: v.IndexDefinition.Name, Generation: v.Generation}
	deadline := func() time.Time {
		d, _ := ctx.Deadline()
		short := time.Now().Add(30 * time.Second)
		if d.Before(short) {
			return d
		}
		return short
	}
	search := func(query []float32) (public.SearchResponseV1, error) {
		return client.VectorSearchStrictV1(ctx, public.SearchRequestV1{Version: 1, Generation: generation, Query: query, Metric: public.MetricCosineV1, TopK: 1, Probes: 1, EfSearch: 8, Consistency: public.ConsistencyGenerationSnapshotV1, Limits: public.SearchLimitsV1{RequestBytes: 1 << 20, CandidateBytes: 8 << 20, ResponseBytes: 1 << 20, MergeEntries: 8}, Deadline: deadline()})
	}
	// Read-only recovery observations may retry; writes below never do.
	err = fixturePollV1(ctx, func() (bool, error) {
		var e error
		report.Before, e = search([]float32{1, 0})
		if e != nil {
			return false, e
		}
		if e = fixtureNativeGraphSearchV1(report.Before); e != nil {
			return false, e
		}
		if len(report.Before.Neighbors) != 1 || report.Before.Neighbors[0].ID != "seed-x" || report.Before.Neighbors[0].Score != 1 {
			return false, fmt.Errorf("exact seed cosine oracle mismatch")
		}
		return true, nil
	})
	if err != nil {
		return
	}
	request := public.InsertRequestV1{Version: 1, Generation: generation, IdempotencyKey: []byte(requestID + "/fresh"), ID: []byte(requestID + "-fresh-y"), Vector: []float32{0, 1}, Document: []byte(`{"embedding":[0,1],"kind":"fresh"}`), Deadline: deadline()}
	report.Insert, err = client.VectorInsertV1(ctx, request)
	if err != nil {
		return
	}
	if err = public.ValidateInsertResponseV1(request, report.Insert); err != nil {
		return
	}
	request.Deadline = deadline()
	report.Retry, err = client.VectorInsertV1(ctx, request)
	if err != nil {
		return
	}
	if err = public.ValidateInsertResponseV1(request, report.Retry); err != nil {
		return
	}
	if report.Retry.CommitIndex <= report.Insert.CommitIndex || report.Retry.LiveRevision != report.Insert.LiveRevision || report.Retry.VisibleID != report.Insert.VisibleID || report.Retry.OwnerGroup != report.Insert.OwnerGroup || report.Retry.PartitionID != report.Insert.PartitionID {
		err = fmt.Errorf("retry lacks new consensus evidence or stable live identity")
		return
	}
	if err = c.fixturePrefixV1(ctx, report.Retry.CommitIndex); err != nil {
		return
	}
	report.After, err = search([]float32{0, 1})
	if err != nil {
		return
	}
	if err = fixtureNativeGraphSearchV1(report.After); err != nil {
		return
	}
	if len(report.After.Neighbors) != 1 || report.After.Neighbors[0].ID != string(request.ID) || report.After.Neighbors[0].Score != 1 {
		err = fmt.Errorf("exact fresh-write cosine oracle mismatch")
		return
	}
	err = fixturePollV1(ctx, func() (bool, error) {
		report.Readiness = nil
		for _, node := range c.config.Nodes {
			state, e := c.ReadinessV1(ctx, node.ID)
			report.Readiness = append(report.Readiness, state)
			if e != nil {
				return false, e
			}
			if !state.Ready || state.VectorPhase != "active" {
				return false, nil
			}
		}
		return true, nil
	})
	return
}

func fixtureNativeGraphSearchV1(response public.SearchResponseV1) error {
	c := response.Counters
	if c.SelectedPartitions == 0 || c.ExactScanPartitions != 0 || c.HNSWServedPartitions != c.SelectedPartitions {
		return fmt.Errorf("fixture requires native HNSW for every selected partition: selected=%d hnsw=%d exact=%d", c.SelectedPartitions, c.HNSWServedPartitions, c.ExactScanPartitions)
	}
	return nil
}
