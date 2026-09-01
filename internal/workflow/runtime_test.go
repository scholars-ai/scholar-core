package workflow

import (
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestNodeOrderAndValidation(t *testing.T) {
	if nodeIndex("source_fetch") != 0 || nodeIndex("human_review") != len(nodeOrder)-1 {
		t.Fatalf("unexpected node order: %v", nodeOrder)
	}
	if nodeIndex("missing") != -1 {
		t.Fatal("unknown node must return -1")
	}
	if err := validateNode("article_write"); err != nil {
		t.Fatalf("known node rejected: %v", err)
	}
	if err := validateNode("missing"); err == nil {
		t.Fatal("unknown node accepted")
	}
}

func TestParseUUIDsDeduplicatesAndIgnoresInvalidValues(t *testing.T) {
	one := uuid.New()
	two := uuid.New()
	got := parseUUIDs([]any{one.String(), "bad", two, one.String(), uuid.Nil.String()})
	if len(got) != 2 || got[0] != one || got[1] != two {
		t.Fatalf("unexpected UUIDs: %#v", got)
	}
}

func TestIntersectUUIDsPreservesLeftOrder(t *testing.T) {
	one := uuid.New()
	two := uuid.New()
	three := uuid.New()
	got := intersectUUIDs([]uuid.UUID{three, one, two, one}, []uuid.UUID{one, three})
	if len(got) != 2 || got[0] != three || got[1] != one {
		t.Fatalf("unexpected intersection: %#v", got)
	}
}

func TestCountsJSONContainsDynamicFunnelCounts(t *testing.T) {
	got := string(countsJSON(100, 10, 80, 5, 5, 10))
	for _, field := range []string{`"input":100`, `"accepted":10`, `"rejected":80`, `"skipped":5`, `"failed":5`, `"output":10`} {
		if !strings.Contains(got, field) {
			t.Errorf("counts missing %s: %s", field, got)
		}
	}
}
