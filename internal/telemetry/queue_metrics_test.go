package telemetry

import "testing"

func TestQueueDepths(t *testing.T) {
	tests := []struct {
		name         string
		queueLength  int64
		visible      int64
		wantWaiting  int64
		wantInFlight int64
		wantCurrent  int64
	}{
		{name: "empty", queueLength: 0, visible: 0},
		{
			name: "waiting and in flight", queueLength: 7, visible: 4,
			wantWaiting: 4, wantInFlight: 3, wantCurrent: 7,
		},
		{
			name: "all in flight", queueLength: 2, visible: 0,
			wantInFlight: 2, wantCurrent: 2,
		},
		{
			name: "invalid visible count is clamped", queueLength: 2, visible: 3,
			wantWaiting: 2, wantCurrent: 2,
		},
		{name: "negative values are clamped", queueLength: -1, visible: -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			waiting, inFlight, current := queueDepths(tt.queueLength, tt.visible)
			if waiting != tt.wantWaiting || inFlight != tt.wantInFlight || current != tt.wantCurrent {
				t.Fatalf(
					"queueDepths(%d, %d) = (%d, %d, %d), want (%d, %d, %d)",
					tt.queueLength, tt.visible, waiting, inFlight, current,
					tt.wantWaiting, tt.wantInFlight, tt.wantCurrent,
				)
			}
		})
	}
}
