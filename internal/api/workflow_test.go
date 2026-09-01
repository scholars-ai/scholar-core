package api

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/scholars-ai/scholar-core/internal/db/dbgen"
)

func TestReplayScopeAndConfigOverridesConvertToWorkflowPayload(t *testing.T) {
	item := uuid.New()
	useCurrent := true
	threshold := float32(72)
	model := "judge-replay"
	scope := replayScopeMap(ReplayScope{Mode: SelectedItems, ItemIds: &[]uuid.UUID{item}, UseCurrentInput: &useCurrent})
	if scope["mode"] != "selected_items" || scope["useCurrentInput"] != true {
		t.Fatalf("unexpected scope: %#v", scope)
	}
	if ids, ok := scope["itemIds"].([]string); !ok || len(ids) != 1 || ids[0] != item.String() {
		t.Fatalf("unexpected item ids: %#v", scope["itemIds"])
	}
	overrides := configOverridesMap(&WorkflowConfigOverrides{Model: &model, TopicPassThreshold: &threshold})
	if overrides["model"] != model || overrides["topicPassThreshold"] != float64(72) {
		t.Fatalf("unexpected overrides: %#v", overrides)
	}
}

func TestWorkflowStageMetricsIncludesCountsPassRateAndReasons(t *testing.T) {
	nodeID := uuid.New()
	itemID := uuid.New()
	var score pgtype.Numeric
	if err := score.Scan("81.5"); err != nil {
		t.Fatal(err)
	}
	reasons := map[string]interface{}{}
	metrics := workflowStageMetrics("topic_evaluate", dbgen.WorkflowNodeRun{
		ID: nodeID, NodeKey: "topic_evaluate", Counts: json.RawMessage(`{"input":4,"accepted":2,"rejected":2,"output":2}`),
	}, []dbgen.WorkflowItemDecision{{NodeRunID: nodeID, ItemID: itemID, Decision: "accepted", ReasonCode: "quality_ok", TotalScore: score}}, reasons, "base")
	if metrics.InputCount == nil || *metrics.InputCount != 4 || metrics.PassRate == nil || *metrics.PassRate != 0.5 {
		t.Fatalf("unexpected metrics: %#v", metrics)
	}
	if reasons["base:topic_evaluate:quality_ok"] != 1 {
		t.Fatalf("unexpected reason counts: %#v", reasons)
	}
	if metrics.ScoreDistribution == nil {
		t.Fatal("score distribution missing")
	}
}

func TestBuildWorkflowSourcePayloadCarriesCascadeRun(t *testing.T) {
	runID := uuid.New()
	sourceID := uuid.New()
	got := buildWorkflowSourcePayload(sourceID, runID)

	if got["sourceId"] != sourceID.String() {
		t.Fatalf("sourceId = %v, want %s", got["sourceId"], sourceID)
	}
	if got["cascade"] != true {
		t.Fatalf("cascade = %v, want true", got["cascade"])
	}
	if got["workflowRunId"] != runID.String() {
		t.Fatalf("workflowRunId = %v, want %s", got["workflowRunId"], runID)
	}
}

func TestReduceWorkflowNodeEventsUsesLatestEvent(t *testing.T) {
	node := "topic_scout"
	events := []dbgen.WorkflowEvent{
		{NodeKey: node, EventType: "queued", Status: "queued"},
		{NodeKey: node, EventType: "started", Status: "running"},
		{NodeKey: node, EventType: "succeeded", Status: "succeeded"},
	}
	if got := reduceWorkflowNodeStatus(events, node); got != "succeeded" {
		t.Fatalf("status = %q, want succeeded", got)
	}
}

func TestWorkflowStreamPathBypassesRequestTimeout(t *testing.T) {
	if !isWorkflowStreamPath("/api/v1/workflow/runs/8c18f453-36ff-44d2-99ee-ea8e29290cf1/stream") {
		t.Fatal("workflow SSE path must bypass request timeout")
	}
	if isWorkflowStreamPath("/api/v1/workflow/runs") {
		t.Fatal("regular workflow API must retain request timeout")
	}
}

func TestWorkflowSnapshotChecksumValidation(t *testing.T) {
	payload := []byte(`{ "articleIds": ["a"], "node": "article_evaluate" }`)
	canonical := []byte(`{"articleIds":["a"],"node":"article_evaluate"}`)
	hash := sha256.Sum256(canonical)
	if !workflowSnapshotChecksumValid(payload, fmt.Sprintf("%x", hash[:])) {
		t.Fatal("valid snapshot checksum rejected")
	}
	if workflowSnapshotChecksumValid(append(payload, 'x'), fmt.Sprintf("%x", hash[:])) {
		t.Fatal("tampered snapshot checksum accepted")
	}
}

func TestWorkflowComparisonIncludesDurationAndUsage(t *testing.T) {
	baseID, otherID := uuid.New(), uuid.New()
	started := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	completedBase := started.Add(4 * time.Second)
	completedOther := started.Add(7 * time.Second)
	baseNode := dbgen.WorkflowNodeRun{ID: uuid.New(), NodeKey: "article_evaluate", Counts: json.RawMessage(`{"input":2,"accepted":1,"rejected":1}`), StartedAt: pgtype.Timestamptz{Time: started, Valid: true}, CompletedAt: pgtype.Timestamptz{Time: completedBase, Valid: true}}
	otherNode := dbgen.WorkflowNodeRun{ID: uuid.New(), NodeKey: "article_evaluate", Counts: json.RawMessage(`{"input":2,"accepted":2}`), StartedAt: pgtype.Timestamptz{Time: started, Valid: true}, CompletedAt: pgtype.Timestamptz{Time: completedOther, Valid: true}}
	baseUsage := map[string]workflowStageUsage{"article_evaluate": {tokens: 120, cost: 0.12, hasTokens: true, hasCost: true}}
	otherUsage := map[string]workflowStageUsage{"article_evaluate": {tokens: 180, cost: 0.18, hasTokens: true, hasCost: true}}
	snapshotID := uuid.New()
	baseRun := dbgen.WorkflowRun{ID: baseID, InputSnapshotID: uuid.NullUUID{UUID: snapshotID, Valid: true}, StartedAt: pgtype.Timestamptz{Time: started, Valid: true}, CompletedAt: pgtype.Timestamptz{Time: completedBase, Valid: true}}
	otherRun := dbgen.WorkflowRun{ID: otherID, InputSnapshotID: uuid.NullUUID{UUID: snapshotID, Valid: true}, StartedAt: pgtype.Timestamptz{Time: started, Valid: true}, CompletedAt: pgtype.Timestamptz{Time: completedOther, Valid: true}}
	comparison := compareWorkflowRunsWithUsage(baseRun, otherRun, []dbgen.WorkflowNodeRun{baseNode}, []dbgen.WorkflowNodeRun{otherNode}, nil, nil, nil, nil, baseUsage, otherUsage)
	baseMetrics := comparison.Stages["article_evaluate"].Base
	if baseMetrics.DurationSeconds == nil || *baseMetrics.DurationSeconds != 4 || baseMetrics.TokenCount == nil || *baseMetrics.TokenCount != 120 || baseMetrics.Cost == nil || *baseMetrics.Cost != 0.12 {
		t.Fatalf("base metrics missing duration or usage: %#v", baseMetrics)
	}
	if comparison.Cost == nil || (*comparison.Cost)["base"].(map[string]interface{})["tokenCount"] != 120 {
		t.Fatalf("run usage missing from comparison: %#v", comparison.Cost)
	}
}
