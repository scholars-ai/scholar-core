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

func TestDefaultSettingsUseFixedContentCadence(t *testing.T) {
	settings := DefaultSettings()

	if settings.SourceFetch.DefaultIntervalMinutes != 120 {
		t.Fatalf("source fetch interval = %d, want 120", settings.SourceFetch.DefaultIntervalMinutes)
	}
	wantScoutTimes := []string{"00:00", "04:00", "08:00", "12:00", "16:00", "20:00"}
	if len(settings.TopicScout.Times) != len(wantScoutTimes) {
		t.Fatalf("topic scout times = %v, want %v", settings.TopicScout.Times, wantScoutTimes)
	}
	for i, want := range wantScoutTimes {
		if settings.TopicScout.Times[i] != want {
			t.Fatalf("topic scout time[%d] = %q, want %q", i, settings.TopicScout.Times[i], want)
		}
	}
	if !settings.ArticleWrite.Enabled || settings.ArticleWrite.MaxTopics != 3 {
		t.Fatalf("unexpected article write defaults: %#v", settings.ArticleWrite)
	}
	if got := settings.ArticleWrite.Times; len(got) != 3 || got[0] != "00:00" || got[1] != "08:00" || got[2] != "16:00" {
		t.Fatalf("unexpected article write times: %v", got)
	}
}
