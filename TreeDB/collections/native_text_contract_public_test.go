package collections

import (
	"errors"
	"math"
	"strings"
	"testing"

	treedb "github.com/snissn/gomap/TreeDB"
	backenddb "github.com/snissn/gomap/TreeDB/db"
	"github.com/snissn/gomap/TreeDB/internal/memtable"
	"github.com/snissn/gomap/TreeDB/node"
)

// The native catalog must preserve the maintained analyzer/ranking contract,
// including validation before any accepted schema effect.
func TestNativeTextAnalyzerPublicContract(t *testing.T) {
	for _, version := range []TextIndexVersion{TextIndexVersionV1, TextIndexVersionV2} {
		t.Run(string(version), func(t *testing.T) {
			_, _, c := openNativeTextPublic(t, t.TempDir(), treedb.ProfileNoWALFast, true, version)
			for _, bad := range []TextIndexDefinition{
				{Version: "unknown", Fields: []TextIndexField{{Field: "title"}}},
				{Fields: []TextIndexField{{Field: "title", Weight: -1}}},
				{Fields: []TextIndexField{{Field: "title", Weight: math.NaN()}}},
				{Fields: []TextIndexField{{Field: "title", Weight: math.Inf(1)}}},
				{Analyzer: "unknown", Fields: []TextIndexField{{Field: "title"}}},
				{AnalyzerOptions: &TextAnalyzerOptions{Stemmer: "english"}, Fields: []TextIndexField{{Field: "title"}}},
				{AnalyzerOptions: &TextAnalyzerOptions{Synonyms: map[string][]string{"refund": {"return"}}}, Fields: []TextIndexField{{Field: "title"}}},
			} {
				before, err := c.nativeJSON.Capture(c.name)
				if err != nil {
					t.Fatal(err)
				}
				sequence := before.capture.LogicalSequence()
				if err = before.Close(); err != nil {
					t.Fatal(err)
				}
				bad.Name = "invalid"
				if _, _, err = c.CreateTextIndex(bad); err == nil {
					t.Fatalf("accepted invalid definition %+v", bad)
				}
				after, err := c.nativeJSON.Capture(c.name)
				if err != nil {
					t.Fatal(err)
				}
				if after.capture.LogicalSequence() != sequence {
					t.Error("invalid declaration published a logical effect")
				}
				for _, def := range after.meta.TextIndexes {
					if def.Name == bad.Name {
						t.Error("invalid declaration installed metadata")
					}
				}
				if err = after.Close(); err != nil {
					t.Fatal(err)
				}
			}
			def := TextIndexDefinition{Name: "weighted", Version: version, StorePositions: version == TextIndexVersionV2,
				Fields:          []TextIndexField{{Field: "title", Weight: 8}, {Field: "body"}},
				AnalyzerOptions: &TextAnalyzerOptions{StopWords: []string{"the", "why"}}}
			if _, _, err := c.CreateTextIndex(def); err != nil {
				t.Fatal(err)
			}
			if _, err := c.InsertBatch([][]byte{[]byte("body-heavy"), []byte("title-hit")}, [][]byte{
				[]byte(`{"title":"plain","body":"the refund refund refund refund refund refund"}`),
				[]byte(`{"title":"refund","body":"other"}`),
			}); err != nil {
				t.Fatal(err)
			}
			got, err := c.SearchText(TextSearchOptions{IndexName: "weighted", Query: "why refund", QueryMode: TextSearchQueryModeLiteral, TopK: 2})
			if err != nil {
				t.Fatal(err)
			}
			if len(got.Results) != 2 || string(got.Results[0].DocumentID) != "title-hit" || got.Results[0].Score <= got.Results[1].Score || got.Stats.QueryTerms != 1 || got.Stats.FullDocumentScanFallbacks != 0 {
				t.Fatalf("weighted/analyzed response %+v", got)
			}
			empty, err := c.SearchText(TextSearchOptions{IndexName: "weighted", Query: "the why", QueryMode: TextSearchQueryModeLiteral, TopK: 2})
			if err != nil || len(empty.Results) != 0 || empty.Stats.QueryTerms != 0 {
				t.Fatalf("stopword response %+v err=%v", empty, err)
			}
			if version == TextIndexVersionV2 {
				phrase, err := c.SearchText(TextSearchOptions{IndexName: "weighted", Phrase: &TextSearchPhraseQuery{Query: "the refund"}, TopK: 2})
				if err != nil || len(phrase.Results) != 2 {
					t.Fatalf("stopword phrase %+v err=%v", phrase, err)
				}
			}
		})
	}
}

// Inject malformed records through the real NoWAL conditional owner, then run
// the public inspector over the native catalog. This is corruption validation,
// not an application mutation/recovery API or a durability claim.
func TestNativeTextMalformedPublicCatalog(t *testing.T) {
	for _, tc := range []struct {
		name, root string
		key, value []byte
	}{
		{"format", collectionTextV2NormBlocksRootName("docs", "content"), encodeTextV2FormatKey(), []byte{99}},
		{"stats generation", collectionTextV2TermsRootName("docs", "content"), encodeTextV2CorpusStatsKey(), encodeTextV2CorpusStatsValue(textV2CorpusStatsValue{StatsGeneration: 99, DocumentCount: 1})},
		{"norm generation", collectionTextV2NormBlocksRootName("docs", "content"), encodeTextV2BlockKey(1), encodeTextV2NormBlockValue(textV2NormBlockValue{BlockStart: 1, BlockSize: textV2DefaultNormBlockSize, FieldCount: 1, Entries: []textV2NormBlockEntry{{Ordinal: 1, Generation: 99, FieldLengths: []uint32{2}}}})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, c := openNativeTextPublic(t, t.TempDir(), treedb.ProfileNoWALFast, true)
			if _, err := c.Insert([]byte("d1"), []byte(`{"content":"refund policy"}`)); err != nil {
				t.Fatal(err)
			}
			if _, err := c.TextIndexStorageStats("content"); err != nil {
				t.Fatal(err)
			}
			handle := c.nativeJSON.db
			tx, err := handle.BeginNativeConditionalTxn()
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := tx.Close(); err != nil {
					t.Error(err)
				}
			}()
			base := tx.State().(*nativeEngineVersion)
			current, ok := base.names.Get(c.name)
			if !ok {
				t.Fatal("missing collection")
			}
			name := strings.TrimPrefix(tc.root, "docs/")
			domain, ok := current.domains.Get(name)
			if !ok {
				t.Fatalf("missing domain %s", name)
			}
			next, err := prepareNativeNamedDomains(base, []nativeNamedCollectionChange{{collection: current, sourceGeneration: current.documentGeneration, domains: []nativeNamedDomainChange{{name: name, expectedInterpretation: domain.interpretation, entries: []memtable.COWMutation{{Key: tc.key, Value: tc.value, Flags: node.FlagInline}}}}}})
			if err != nil {
				if next != nil {
					err = errors.Join(err, finishPrivateNative(next.Release()))
				}
				t.Fatal(err)
			}
			pub := &backenddb.NativePreparedPublication{Basis: base, State: next, StartSequence: tx.LogicalSequence()}
			result, err := tx.CommitNative(pub, nil, false)
			if !result.Consumed {
				err = errors.Join(err, cancelPrivateNativePublication(pub))
			}
			if err != nil || !result.LogicalInstalled {
				t.Fatalf("fault install %+v err=%v", result, err)
			}
			if _, err = c.TextIndexStorageStats("content"); !errors.Is(err, ErrTextIndexStorageCorrupt) {
				t.Fatalf("malformed catalog err=%v", err)
			}
		})
	}
}
