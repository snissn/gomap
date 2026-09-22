package raftplacement

import (
    "errors"
    "fmt"

    "github.com/snissn/gomap/TreeDB/internal/raftcluster"
    "github.com/snissn/gomap/TreeDB/internal/sourcepartition"
)

const (
    SourceShardMapFormatV2 = sourcepartition.SourceShardMapFormatV2
    DocumentIDTokenAlgorithmV2 = sourcepartition.DocumentIDTokenAlgorithmV2
    MaxSourceShardsV2 = sourcepartition.MaxSourceShardsV2
    MaxSourceDocumentIDBytesV2 = sourcepartition.MaxSourceDocumentIDBytesV2
    MaxSourceImportBytesV2 = sourcepartition.MaxSourceImportBytesV2
)

var ErrInvalidSourceShardMapV2 = sourcepartition.ErrInvalidSourceShardMapV2

// SourceShardV2 owns a canonical document-ID token range, independently of ANN
// domains and local ordinals. Catalog admission remains in raftplacement.
type SourceShardV2 struct {
    ShardID string
    GroupID raftcluster.GroupID
    Start uint64
    End uint64
}

type SourceShardMapV2 struct {
    Format string
    Collection CollectionRefV1
    Epoch uint64
    TokenAlgorithm string
    Shards []SourceShardV2
    Digest string
}

// ResolvedSourceShardMapV2 adds known catalog-group validation to pure source
// map validation. It still requires committed catalog/generation admission;
// constructing a map or validating its digest alone grants no routing authority.
type ResolvedSourceShardMapV2 struct {
    source sourcepartition.ResolvedSourceShardMapV2
}

func DocumentIDTokenV2(id []byte) (uint64,error) { return sourcepartition.DocumentIDTokenV2(id) }

func sourceShardMapPureV2(input SourceShardMapV2) sourcepartition.SourceShardMapV2 {
    out:=sourcepartition.SourceShardMapV2{Format:input.Format,Collection:sourcepartition.CollectionRefV2{Database:input.Collection.Database,Catalog:input.Collection.Catalog,Collection:input.Collection.Collection},Epoch:input.Epoch,TokenAlgorithm:input.TokenAlgorithm,Digest:input.Digest}
    out.Shards=make([]sourcepartition.SourceShardV2,len(input.Shards))
    for i,shard:=range input.Shards { out.Shards[i]=sourcepartition.SourceShardV2{ShardID:shard.ShardID,GroupID:string(shard.GroupID),Start:shard.Start,End:shard.End} }
    return out
}
func sourceShardMapFromPureV2(input sourcepartition.SourceShardMapV2) SourceShardMapV2 {
    out:=SourceShardMapV2{Format:input.Format,Collection:CollectionRefV1{Database:input.Collection.Database,Catalog:input.Collection.Catalog,Collection:input.Collection.Collection},Epoch:input.Epoch,TokenAlgorithm:input.TokenAlgorithm,Digest:input.Digest}
    out.Shards=make([]SourceShardV2,len(input.Shards))
    for i,shard:=range input.Shards { out.Shards[i]=SourceShardV2{ShardID:shard.ShardID,GroupID:raftcluster.GroupID(shard.GroupID),Start:shard.Start,End:shard.End} }
    return out
}

func (c ResolvedCatalogV1) validateSourceGroupsV2(input SourceShardMapV2) error {
    if len(input.Shards)==0 || len(input.Shards)>MaxSourceShardsV2 { return fmt.Errorf("%w: shard count",ErrInvalidSourceShardMapV2) }
    if err:=validateCollectionRef(input.Collection);err!=nil{return errors.Join(ErrInvalidSourceShardMapV2,err)}
    for _,shard:=range input.Shards {
        if _,ok:=c.groups[shard.GroupID]; !ok { return errors.Join(ErrInvalidSourceShardMapV2,ErrUnknownGroup,fmt.Errorf("source shard %q group %q is absent from catalog",shard.ShardID,shard.GroupID)) }
    }
    return nil
}

func (c ResolvedCatalogV1) CanonicalSourceShardMapV2(input SourceShardMapV2) (SourceShardMapV2,error) {
    if err:=c.validateSourceGroupsV2(input);err!=nil{return SourceShardMapV2{},err}
    canonical,err:=sourcepartition.CanonicalSourceShardMapV2(sourceShardMapPureV2(input))
    if err!=nil{return SourceShardMapV2{},err}
    return sourceShardMapFromPureV2(canonical),nil
}
func (c ResolvedCatalogV1) ValidateSourceShardMapV2(input SourceShardMapV2) (ResolvedSourceShardMapV2,error) {
    if err:=c.validateSourceGroupsV2(input);err!=nil{return ResolvedSourceShardMapV2{},err}
    resolved,err:=sourcepartition.ValidateSourceShardMapV2(sourceShardMapPureV2(input))
    if err!=nil{return ResolvedSourceShardMapV2{},err}
    return ResolvedSourceShardMapV2{source:resolved},nil
}
func (m ResolvedSourceShardMapV2) Collection() CollectionRefV1 { ref:=m.source.Collection();return CollectionRefV1{Database:ref.Database,Catalog:ref.Catalog,Collection:ref.Collection} }
func (m ResolvedSourceShardMapV2) Epoch() uint64 { return m.source.Epoch() }
func (m ResolvedSourceShardMapV2) Digest() string { return m.source.Digest() }
func (m ResolvedSourceShardMapV2) ResolveDocumentID(id []byte) (SourceShardV2,error) {
    shard,err:=m.source.ResolveDocumentID(id)
    if err!=nil{return SourceShardV2{},err}
    return SourceShardV2{ShardID:shard.ShardID,GroupID:raftcluster.GroupID(shard.GroupID),Start:shard.Start,End:shard.End},nil
}
func (m ResolvedSourceShardMapV2) ValidateImportIDs(shardID string,ids [][]byte) error { return m.source.ValidateImportIDs(shardID,ids) }

// SourceMapV2 returns immutable pure lookup state for collection-side ownership
// checks. This is not a catalog publication or source-root authority certificate.
func (m ResolvedSourceShardMapV2) SourceMapV2() sourcepartition.ResolvedSourceShardMapV2 { return m.source }
