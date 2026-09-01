package telemetry

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
)

func TestProbeEndpointReportsReachability(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := probeEndpoint(ctx, listener.Addr().String()); err != nil {
		t.Fatalf("probe should succeed for a live grpc endpoint: %v", err)
	}

	server.Stop()
	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := probeEndpoint(ctx, listener.Addr().String()); err == nil {
		t.Fatal("probe should fail after the endpoint is stopped")
	}
}
