package api

import (
	"encoding/json"
	"math"
	"testing"
	"time"
)

func intPtr(value int) *int { return &value }

func TestPerformanceRawUsesVersionedPlatformWeights(t *testing.T) {
	weights, _ := json.Marshal(map[string]float64{
		"views": 0.1, "likes": 0.2, "favorites": 0.3,
		"comments": 0.1, "shares": 0.1, "follows": 0.2,
	})
	score, err := performanceRaw(EngagementMetrics{
		Views: intPtr(100), Likes: intPtr(20), Favorites: intPtr(10),
		Comments: intPtr(5), Shares: intPtr(2), Follows: intPtr(3),
	}, weights)
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(score-18.3) > 0.0001 {
		t.Fatalf("score = %v, want 18.3", score)
	}
}

func TestDueMetricRemindersOnlyReturnsMissingMatureWindows(t *testing.T) {
	published := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	now := published.Add(4 * 24 * time.Hour)
	snapshots := []MetricSnapshot{{SnapshotWindow: H24}}

	reminders := dueMetricReminders(published, snapshots, now)

	if len(reminders) != 1 || reminders[0].SnapshotWindow != H72 {
		t.Fatalf("reminders = %#v, want only h72", reminders)
	}
}

func TestValidateMetricInputRequiresAtLeastOneNonNegativeMetric(t *testing.T) {
	now := time.Now().UTC()
	if err := validateMetricInput(H24, now, EngagementMetrics{}); err == nil {
		t.Fatal("expected empty metrics to fail")
	}
	negative := -1
	if err := validateMetricInput(H24, now, EngagementMetrics{Views: &negative}); err == nil {
		t.Fatal("expected negative metric to fail")
	}
	if err := validateMetricInput(H24, now, EngagementMetrics{Views: intPtr(0)}); err != nil {
		t.Fatalf("zero metric should be valid: %v", err)
	}
}
