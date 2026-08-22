package api

import (
	"testing"

	"github.com/google/uuid"
	"github.com/scholars-ai/scholar-core/internal/db/dbgen"
)

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
