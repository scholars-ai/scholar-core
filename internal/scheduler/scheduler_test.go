package scheduler

import "testing"

func TestScheduledScoutPayloadBoundsBatch(t *testing.T) {
	payload := scheduledScoutPayload()

	if got := payload["maxItems"]; got != 20 {
		t.Fatalf("maxItems = %v, want 20", got)
	}
}

func TestDefaultSettingsEnableWeeklyReflector(t *testing.T) {
	settings := DefaultSettings()
	if !settings.MemoryReflect.Enabled || settings.MemoryReflect.Weekday != 1 {
		t.Fatalf("unexpected memory reflect defaults: %#v", settings.MemoryReflect)
	}
	if settings.MemoryReflect.Time != "09:00" || settings.MemoryReflect.LookbackDays != 7 {
		t.Fatalf("unexpected memory reflect schedule: %#v", settings.MemoryReflect)
	}
}
