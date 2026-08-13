package scheduler

import "testing"

func TestScheduledScoutPayloadBoundsBatch(t *testing.T) {
	payload := scheduledScoutPayload()

	if got := payload["maxItems"]; got != 20 {
		t.Fatalf("maxItems = %v, want 20", got)
	}
}
