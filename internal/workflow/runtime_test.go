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

func TestNormalizeCreateOptionsAppliesDefaultsAndValidatesNode(t *testing.T) {
	got, err := normalizeCreateOptions(CreateOptions{}, "scheduled")
	if err != nil {
		t.Fatalf("defaults rejected: %v", err)
	}
	if got.TriggerType != "scheduled" || got.StartNode != "source_fetch" {
		t.Fatalf("unexpected defaults: trigger=%q start=%q", got.TriggerType, got.StartNode)
	}
	if _, err := normalizeCreateOptions(CreateOptions{StartNode: "unknown"}, "manual"); err == nil {
		t.Fatal("unknown start node accepted")
	}
}

func TestConfigOverrideValidationRejectsUnknownAndOutOfRangeValues(t *testing.T) {
	if err := validateConfigOverrides(map[string]any{"topicPassThreshold": 72.0, "model": "judge"}); err != nil {
		t.Fatalf("valid overrides rejected: %v", err)
	}
	for _, overrides := range []map[string]any{
		{"unknown": true},
		{"articlePassThreshold": 101.0},
		{"maxConcurrency": 0.0},
		{"weightVersion": 1.5},
		{"model": "   "},
	} {
		if err := validateConfigOverrides(overrides); err == nil {
			t.Fatalf("invalid overrides accepted: %#v", overrides)
		}
	}
}

func TestParseUUIDsDeduplicatesAndIgnoresInvalidValues(t *testing.T) {
	one := uuid.New()
	two := uuid.New()
	got := parseUUIDs([]any{one.String(), "bad", two, one.String(), uuid.Nil.String()})
	if len(got) != 2 || got[0] != one || got[1] != two {
		t.Fatalf("unexpected UUIDs: %#v", got)
	}
	if got = parseUUIDs([]string{one.String(), two.String()}); len(got) != 2 {
		t.Fatalf("string slice was not parsed: %#v", got)
	}
}

func TestReplayScopeValidation(t *testing.T) {
	if err := validateReplayScope(map[string]any{"mode": "full"}); err != nil {
		t.Fatalf("full replay rejected: %v", err)
	}
	if err := validateReplayScope(map[string]any{"mode": "selected_items", "itemIds": []string{uuid.NewString()}}); err != nil {
		t.Fatalf("selected replay rejected: %v", err)
	}
	if err := validateReplayScope(map[string]any{"mode": "selected_items"}); err == nil {
		t.Fatal("selected replay without item IDs accepted")
	}
	if err := validateReplayScope(map[string]any{"mode": "unknown"}); err == nil {
		t.Fatal("unknown replay mode accepted")
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
