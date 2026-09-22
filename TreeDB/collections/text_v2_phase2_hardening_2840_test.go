package collections

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestTextV2Phase2HardeningRandomizedAnalyzerScalarExplain2840(t *testing.T) {
	for seed := int64(11); seed <= 13; seed++ {
		seed := seed
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			d := openTextV2TestDB(t, t.TempDir(), false)
			defer func() { _ = d.Close() }()
			ids := make([][]byte, 160)
			docs := make([][]byte, len(ids))
			allow := make(hybridScalarAllowSet)
			terms := []string{"refund", "policy", "shipping", "support", "chargeback", "invoice"}
			for i := range ids {
				ids[i] = []byte(fmt.Sprintf("doc-%03d", i))
				if i%19 == int(seed%7) {
					allow[string(ids[i])] = struct{}{}
				}
				title := textV2RandomTerms2628(rng, terms, 2+rng.Intn(4))
				body := textV2RandomTerms2628(rng, append(terms, "the", "a"), 5+rng.Intn(8))
				docs[i] = []byte(fmt.Sprintf(`{"title":"%s","body":"%s"}`, title, body))
			}
			col := createTextSearchCollection2627(t, d, "docs", TextIndexDefinition{
				Name:            "lexical",
				Version:         TextIndexVersionV2,
				StorePositions:  true,
				AnalyzerOptions: &TextAnalyzerOptions{StopWords: []string{"the", "a"}},
				Fields:          []TextIndexField{{Field: "title", Weight: 2}, {Field: "body"}},
			}, ids, docs)

			cases := []TextSearchOptions{
				{IndexName: "lexical", Query: "refund policy", Operator: TextSearchOperatorAND, TopK: 7, CandidateLimit: len(ids), MaxPostingsScanned: len(ids) * 8, Explain: true},
				{IndexName: "lexical", Query: "refund OR shipping", Operator: TextSearchOperatorOR, TopK: 7, CandidateLimit: len(ids), MaxPostingsScanned: len(ids) * 8, Explain: true, textV2AllowedDocumentIDs: allow},
				{IndexName: "lexical", Phrase: &TextSearchPhraseQuery{Query: "the refund policy", Slop: 2}, TopK: 7, CandidateLimit: len(ids), MaxPostingsScanned: len(ids) * 8, Explain: true, textV2AllowedDocumentIDs: allow},
			}
			for i, opts := range cases {
				exhaustiveOpts := opts
				exhaustiveOpts.Explain = false
				exhaustiveOpts.textV2DisableBlockMax = true
				exhaustive, err := col.searchText(exhaustiveOpts, textSearchResultScoreOnly)
				if err != nil {
					t.Fatalf("case %d exhaustive: %v", i, err)
				}
				got, err := col.searchText(opts, textSearchResultScoreOnly)
				if err != nil {
					t.Fatalf("case %d explain search: %v", i, err)
				}
				assertTextSearchParity2627(t, got, exhaustive)
				if got.Explain == nil || got.Stats.DocumentsFetched != 0 || got.Stats.TextStateLookups != 0 || got.Stats.FailClosed != 0 {
					t.Fatalf("case %d response=%+v explain=%+v want explain without document fetch/state/fail-closed", i, got.Stats, got.Explain)
				}
			}
		})
	}
}
