package api

import (
	"testing"
	"time"

	"github.com/scholars-ai/scholar-core/internal/scheduler"
)

func TestNextDailyRun(t *testing.T) {
	now := time.Date(2026, 8, 18, 9, 30, 0, 0, time.UTC)
	got := nextDailyRun([]string{"00:00", "08:00", "16:00"}, "UTC", now)
	want := time.Date(2026, 8, 18, 16, 0, 0, 0, time.UTC)
	if got == nil || !got.Equal(want) {
		t.Fatalf("next run = %v, want %v", got, want)
	}

	afterLast := time.Date(2026, 8, 18, 23, 0, 0, 0, time.UTC)
	got = nextDailyRun([]string{"00:00", "08:00", "16:00"}, "UTC", afterLast)
	want = time.Date(2026, 8, 19, 0, 0, 0, 0, time.UTC)
	if got == nil || !got.Equal(want) {
		t.Fatalf("next-day run = %v, want %v", got, want)
	}
}

func TestBuildPipelineStagesUsesFixedCadenceAndQualityCounts(t *testing.T) {
	settings := scheduler.DefaultSettings()
	now := time.Date(2026, 8, 18, 9, 30, 0, 0, time.UTC)
	stages := buildPipelineStages(pipelineCounts{
		RawTotal: 20, RawNew: 5, RawClustered: 14, RawDiscarded: 1,
		TopicTotal: 8, TopicScored: 3, TopicPassed: 4, TopicRejected: 1,
		ArticleTotal: 12, ArticleReady: 4, ArticlePassed: 6, ArticleRejected: 1, ArticleRewrites: 2,
	}, settings, nil, now)

	if len(stages) != 3 {
		t.Fatalf("stage count = %d, want 3", len(stages))
	}
	if stages[0].CadenceMinutes != 120 || stages[1].CadenceMinutes != 240 || stages[2].CadenceMinutes != 480 {
		t.Fatalf("unexpected cadence: %#v", stages)
	}
	if stages[1].Ready != 3 || stages[1].Passed != 4 || stages[1].Failed != 1 {
		t.Fatalf("unexpected topic stage: %#v", stages[1])
	}
	if stages[2].Rewrites != 2 || stages[2].Ready != 4 {
		t.Fatalf("unexpected article stage: %#v", stages[2])
	}
}
