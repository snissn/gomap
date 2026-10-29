package nativewire

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/snissn/gomap/TreeDB/collections"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	iwire "github.com/snissn/gomap/TreeDB/internal/nativewire"
)

func clientVectorCreateDefaultFixture(strategy collections.VectorIndexStrategy, text bool) collections.CollectionMeta {
	meta := collections.CollectionMeta{Name: "docs", Options: collections.CollectionOptions{DocumentFormat: collections.DocumentFormatJSON}}
	if text {
		meta = textMetadataV7Fixture(collections.DocumentFormatJSON, collections.TextIndexVersionV2)
	}
	meta.VectorIndexes = []collections.VectorIndexDefinition{{
		Name: "embedding", Field: "embedding", Dimensions: 2, Strategy: strategy,
		QuantizedIndexes: []collections.QuantizedVectorIndexDefinition{{Name: "embedding.scalar_u8"}},
	}}
	return meta
}

func TestClientCreateCollectionVectorStrategyDefaults(t *testing.T) {
	for _, strategy := range []collections.VectorIndexStrategy{"", collections.VectorIndexStrategyColumnGraph} {
		for _, text := range []bool{false, true} {
			t.Run(fmt.Sprintf("strategy=%q/text=%t", strategy, text), func(t *testing.T) {
				client, mgr, _ := serveCommandWALCollectionPipe(t)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				if err := client.Hello(ctx); err != nil {
					t.Fatal(err)
				}
				input := clientVectorCreateDefaultFixture(strategy, text)
				want, err := collections.NormalizeVectorIndexDefinitionV1(input.VectorIndexes[0])
				if err != nil {
					t.Fatal(err)
				}
				created, err := client.CreateCollection(ctx, input)
				if err != nil {
					t.Fatalf("CreateCollection: %v", err)
				}
				if !reflect.DeepEqual(input, clientVectorCreateDefaultFixture(strategy, text)) {
					t.Fatalf("CreateCollection mutated caller metadata: %+v", input)
				}
				if want.Strategy != collections.VectorIndexStrategyColumnGraph || !reflect.DeepEqual(created.VectorIndexes, []collections.VectorIndexDefinition{want}) {
					t.Fatalf("created vector indexes=%+v want=%+v", created.VectorIndexes, want)
				}
				listed, err := client.ListCollections(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if len(listed) != 1 || !reflect.DeepEqual(listed[0].VectorIndexes, created.VectorIndexes) || !reflect.DeepEqual(listed[0].TextIndexes, created.TextIndexes) {
					t.Fatalf("listed metadata=%+v want=%+v", listed, created)
				}
				local, err := mgr.OpenCollection(input.Name)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(local.Meta().VectorIndexes, created.VectorIndexes) {
					t.Fatalf("catalog vector indexes=%+v want=%+v", local.Meta().VectorIndexes, created.VectorIndexes)
				}
				if text && !reflect.DeepEqual(created.TextIndexes, input.TextIndexes) {
					t.Fatalf("created text indexes=%+v want=%+v", created.TextIndexes, input.TextIndexes)
				}
			})
		}
	}
}

func TestClientCreateCollectionRejectedVectorStrategiesHaveNoEffects(t *testing.T) {
	for _, strategy := range []collections.VectorIndexStrategy{"native_runtime", "unknown"} {
		for _, text := range []bool{false, true} {
			t.Run(fmt.Sprintf("strategy=%q/text=%t", strategy, text), func(t *testing.T) {
				client, mgr, db := serveCommandWALCollectionPipe(t)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				if err := client.Hello(ctx); err != nil {
					t.Fatal(err)
				}
				fixture := func() collections.CollectionMeta {
					meta := clientVectorCreateDefaultFixture("", text)
					meta.VectorIndexes = append(meta.VectorIndexes, collections.VectorIndexDefinition{
						Name: "other", Field: "other", Dimensions: 2, Strategy: strategy,
					})
					return meta
				}
				input := fixture()
				version, lsn := clientCatalogVersion(t, client, ctx), db.CommandWALNextLSN()
				_, err := client.CreateCollection(ctx, input)
				want := iwire.ErrInvalidCommand
				unsupported := strategy == "native_runtime"
				if unsupported {
					want = iwire.ErrUnsupportedFeature
				}
				if nativeCodeOf(err) != want || errors.Is(err, backenddb.ErrNativeSemanticUnsupported) != unsupported {
					t.Fatalf("rejection=%v code=%d want=%d semantic unsupported=%t", err, nativeCodeOf(err), want, unsupported)
				}
				if !reflect.DeepEqual(input, fixture()) {
					t.Fatalf("rejected CreateCollection mutated caller metadata: %+v", input)
				}
				if db.CommandWALNextLSN() != lsn || clientCatalogVersion(t, client, ctx) != version {
					t.Fatal("rejected vector strategy changed WAL/catalog state")
				}
				if _, err := mgr.OpenCollection(input.Name); err == nil {
					t.Fatal("rejected vector strategy published collection")
				}
			})
		}
	}
}
