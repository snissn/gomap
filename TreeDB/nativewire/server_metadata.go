package nativewire

import (
	"github.com/snissn/gomap/TreeDB/collections"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

func (s *Server) handleCreateCollection(sections []iwire.Section) ([]iwire.Section, error) {
	if err := managerRequired(s.collections); err != nil {
		return nil, err
	}
	if err := s.rejectClusterLocalMutation("create_collection"); err != nil {
		return nil, err
	}
	s.metadataMu.Lock()
	defer s.metadataMu.Unlock()
	replay, remember, err := s.beginMetadataIdempotency(iwire.CommandCreateCollection, sections)
	if err != nil || replay != nil {
		return replay, err
	}
	if err := s.checkCatalogGuard(sections); err != nil {
		return nil, err
	}
	raw, err := metadataSection(sections, iwire.SectionCollectionMeta)
	if err != nil {
		return nil, err
	}
	meta, err := decodeCollectionMeta(raw)
	if err != nil {
		return nil, err
	}
	meta, err = normalizeClientCollectionMeta(meta)
	if err != nil {
		return nil, err
	}
	// Enforce required storage policy for nativewire-created collections.
	meta.Options.DataRootStoragePolicy = collections.RootStorageFast
	meta.Options.IndexStateStoragePolicy = collections.RootStorageFast
	for i := range meta.Indexes {
		meta.Indexes[i].StoragePolicy = collections.RootStorageFast
	}

	logDebug("handleCreateCollection: creating collection with dataPolicy=%s indexPolicy=%s", meta.Options.DataRootStoragePolicy, meta.Options.IndexStateStoragePolicy)

	before, beforeOK := s.catalogMetadataFingerprint()
	created, err := s.collections.CreateCollection(&meta)
	if err != nil {
		logDebug("handleCreateCollection: CreateCollection failed: %v", err)
		return nil, metadataWrap(err)
	}
	s.bumpCatalogVersionIfCatalogMetadataChanged(before, beforeOK)
	logDebug("handleCreateCollection: CreateCollection success")
	return remember([]iwire.Section{{ID: iwire.SectionCollectionMeta, Bytes: encodeCollectionMeta(*created)}}), nil
}

func (s *Server) handleListCollections() ([]iwire.Section, error) {
	if err := managerRequired(s.collections); err != nil {
		return nil, err
	}
	metas, err := s.collections.ListCollections()
	if err != nil {
		return nil, metadataWrap(err)
	}
	return []iwire.Section{{ID: iwire.SectionCollectionMeta, Bytes: encodeCollectionMetaVector(metas)}}, nil
}

func (s *Server) handleCreateIndex(state *connState, sections []iwire.Section) ([]iwire.Section, error) {
	if err := managerRequired(s.collections); err != nil {
		return nil, err
	}
	if err := s.rejectClusterLocalMutation("create_index"); err != nil {
		return nil, err
	}
	s.metadataMu.Lock()
	defer s.metadataMu.Unlock()
	replay, remember, err := s.beginMetadataIdempotency(iwire.CommandCreateIndex, sections)
	if err != nil || replay != nil {
		return replay, err
	}
	if err := s.checkCatalogGuard(sections); err != nil {
		return nil, err
	}
	name, _, err := collectionRefFromSections(state, sections)
	if err != nil {
		return nil, err
	}
	raw, err := metadataSection(sections, iwire.SectionIndexDefinition)
	if err != nil {
		return nil, err
	}
	def, err := decodeIndexDefinition(raw)
	if err != nil {
		return nil, err
	}
	if err := normalizeClientIndexDefinition(def); err != nil {
		return nil, err
	}
	def.StoragePolicy = collections.RootStorageFast
	collection, err := s.collections.OpenCollection(name)
	if err != nil {
		return nil, metadataWrap(err)
	}
	for _, existing := range collection.Meta().Indexes {
		if existing.Name == def.Name {
			return nil, protocolError(iwire.ErrInvalidCommand, "duplicate index %q", def.Name)
		}
	}
	before, beforeOK := s.catalogMetadataFingerprint()
	meta, err := collection.CreateIndex(def)
	if err != nil {
		return nil, metadataWrap(err)
	}
	s.bumpCatalogVersionIfCatalogMetadataChanged(before, beforeOK)
	if state != nil {
		state.cacheCollection(name, collection, s.maxCachedCollections)
	}
	return remember([]iwire.Section{{ID: iwire.SectionCollectionMeta, Bytes: encodeCollectionMeta(*meta)}}), nil
}

func (s *Server) handleListIndexes(state *connState, sections []iwire.Section) ([]iwire.Section, error) {
	_, collection, err := s.openCollectionRef(state, sections)
	if err != nil {
		return nil, err
	}
	meta := collection.Meta()
	return []iwire.Section{{ID: iwire.SectionIndexDefinition, Bytes: encodeIndexDefinitionVector(meta.Indexes)}}, nil
}

func (s *Server) handleDropIndex(state *connState, sections []iwire.Section) ([]iwire.Section, error) {
	if err := managerRequired(s.collections); err != nil {
		return nil, err
	}
	if err := s.rejectClusterLocalMutation("drop_index"); err != nil {
		return nil, err
	}
	s.metadataMu.Lock()
	defer s.metadataMu.Unlock()
	replay, remember, err := s.beginMetadataIdempotency(iwire.CommandDropIndex, sections)
	if err != nil || replay != nil {
		return replay, err
	}
	if err := s.checkCatalogGuard(sections); err != nil {
		return nil, err
	}
	name, _, err := collectionRefFromSections(state, sections)
	if err != nil {
		return nil, err
	}
	raw, err := metadataSection(sections, iwire.SectionIndexName)
	if err != nil {
		return nil, err
	}
	indexName, err := decodeIndexName(raw)
	if err != nil {
		return nil, err
	}
	collection, err := s.collections.OpenCollection(name)
	if err != nil {
		return nil, metadataWrap(err)
	}
	before, beforeOK := s.catalogMetadataFingerprint()
	meta, err := collection.DropIndex(indexName)
	if err != nil {
		return nil, metadataWrap(err)
	}
	s.bumpCatalogVersionIfCatalogMetadataChanged(before, beforeOK)
	if state != nil {
		state.cacheCollection(name, collection, s.maxCachedCollections)
	}
	return remember([]iwire.Section{{ID: iwire.SectionCollectionMeta, Bytes: encodeCollectionMeta(*meta)}}), nil
}

func (s *Server) handleOpenCollection(state *connState, sections []iwire.Section) ([]iwire.Section, error) {
	if err := managerRequired(s.collections); err != nil {
		return nil, err
	}
	name, err := collectionNameFromSections(sections)
	if err != nil {
		return nil, err
	}
	collection, err := s.collections.OpenCollection(name)
	if err != nil {
		return nil, metadataWrap(err)
	}
	handle, err := state.addCollectionHandle(name, collection, s.maxCollectionHandles, s.maxCachedCollections)
	if err != nil {
		return nil, err
	}
	return []iwire.Section{{ID: iwire.SectionCollectionHandle, Bytes: encodeHandle(handle)}}, nil
}

func (s *Server) handleCloseCollection(state *connState, sections []iwire.Section) ([]iwire.Section, error) {
	handle, err := collectionHandleFromSections(state, sections)
	if err != nil {
		return nil, err
	}
	if !state.closeCollectionHandle(handle) {
		return nil, protocolError(iwire.ErrCollectionNotFound, "collection handle %d not found", handle)
	}
	return nil, nil
}
