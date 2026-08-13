package scheduler

import "testing"

func TestScheduledScoutPayloadBoundsBatch(t *testing.T) {
	payload := scheduledScoutPayload()

	if got := payload["maxItems"]; got != scheduledScoutMaxItems {
		t.Fatalf("maxItems = %v, want %d", got, scheduledScoutMaxItems)
	}
}
