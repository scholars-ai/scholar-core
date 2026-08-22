package telemetry

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestStatusRecorderPreservesStreaming(t *testing.T) {
	recorder := &statusRecorder{ResponseWriter: httptest.NewRecorder(), status: http.StatusOK}
	if err := http.NewResponseController(recorder).Flush(); err != nil {
		t.Fatalf("flush through status recorder: %v", err)
	}
}
