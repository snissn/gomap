package nativewire

import (
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

func (s *Server) handleCreateCollection(sections []iwire.Section) ([]iwire.Section, error) {
	if err := managerRequired(s.collections); err != nil {
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
	created, err := s.collections.CreateCollection(&meta)
	if err != nil {
		return nil, metadataWrap(err)
	}
	return []iwire.Section{{ID: iwire.SectionCollectionMeta, Bytes: encodeCollectionMeta(*created)}}, nil
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
	name, err := collectionNameFromSections(sections)
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
	collection, err := s.collections.OpenCollection(name)
	if err != nil {
		return nil, metadataWrap(err)
	}
	meta, err := collection.CreateIndex(def)
	if err != nil {
		return nil, metadataWrap(err)
	}
	return []iwire.Section{{ID: iwire.SectionCollectionMeta, Bytes: encodeCollectionMeta(*meta)}}, nil
}

func (s *Server) handleListIndexes(state *connState, sections []iwire.Section) ([]iwire.Section, error) {
	if err := managerRequired(s.collections); err != nil {
		return nil, err
	}
	name, _, err := collectionRefFromSections(state, sections)
	if err != nil {
		return nil, err
	}
	collection, err := s.collections.OpenCollection(name)
	if err != nil {
		return nil, metadataWrap(err)
	}
	meta := collection.Meta()
	return []iwire.Section{{ID: iwire.SectionIndexDefinition, Bytes: encodeIndexDefinitionVector(meta.Indexes)}}, nil
}

func (s *Server) handleDropIndex(state *connState, sections []iwire.Section) ([]iwire.Section, error) {
	if err := managerRequired(s.collections); err != nil {
		return nil, err
	}
	name, err := collectionNameFromSections(sections)
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
	meta, err := collection.DropIndex(indexName)
	if err != nil {
		return nil, metadataWrap(err)
	}
	return []iwire.Section{{ID: iwire.SectionCollectionMeta, Bytes: encodeCollectionMeta(*meta)}}, nil
}

func (s *Server) handleOpenCollection(state *connState, sections []iwire.Section) ([]iwire.Section, error) {
	if err := managerRequired(s.collections); err != nil {
		return nil, err
	}
	name, err := collectionNameFromSections(sections)
	if err != nil {
		return nil, err
	}
	if _, err := s.collections.OpenCollection(name); err != nil {
		return nil, metadataWrap(err)
	}
	handle, err := state.addCollectionHandle(name, s.maxCollectionHandles)
	if err != nil {
		return nil, err
	}
	return []iwire.Section{{ID: iwire.SectionCollectionHandle, Bytes: encodeHandle(handle)}}, nil
}

func (s *Server) handleCloseCollection(state *connState, sections []iwire.Section) ([]iwire.Section, error) {
	_, wasHandle, err := collectionRefFromSections(state, sections)
	if err != nil {
		return nil, err
	}
	if !wasHandle {
		return nil, protocolError(iwire.ErrInvalidCommand, "close_collection requires a collection handle")
	}
	raw, _, err := singletonSection(sections, iwire.SectionCollectionRef)
	if err != nil {
		return nil, err
	}
	handle, _, err := readUvarint(raw[1:])
	if err != nil {
		return nil, err
	}
	if !state.closeCollectionHandle(CollectionHandle(handle)) {
		return nil, protocolError(iwire.ErrCollectionNotFound, "collection handle %d not found", handle)
	}
	return nil, nil
}
