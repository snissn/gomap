package db

import (
	"errors"
	"strings"
	"testing"

	"github.com/snissn/gomap/TreeDB/internal/rootpublication"
)

func TestStableDBAdjacentPublicationIssueRouting(t *testing.T) {
	tests := []struct {
		field rootpublication.ReachabilityField
		issue string
	}{
		{field: rootpublication.ReachabilityMetaPage, issue: "#3679"},
		{field: rootpublication.ReachabilityUserRoot, issue: "#3679"},
		{field: rootpublication.ReachabilitySystemRoot, issue: "#3679"},
		{field: rootpublication.ReachabilityFreelist, issue: "#3678"},
	}
	for _, test := range tests {
		t.Run(string(test.field), func(t *testing.T) {
			_, err := NewStableDBResourceToken(rootpublication.StableResourceSpec{Reachability: test.field})
			if !errors.Is(err, rootpublication.ErrResourceExcluded) || !strings.Contains(err.Error(), test.issue) {
				t.Fatalf("routing error=%v want ErrResourceExcluded with %s", err, test.issue)
			}
		})
	}
}

func TestOpenSharesValueLogIdentityPinRegistryWithManager(t *testing.T) {
	database, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	registry := database.ValueLogIdentityPinRegistry()
	if registry == nil {
		t.Fatal("opened DB has no value-log identity pin registry")
	}
	if got := database.valueLogManager.StableResourcePinRegistry(); got != registry {
		t.Fatalf("value-log manager registry = %p, DB registry = %p", got, registry)
	}
}
