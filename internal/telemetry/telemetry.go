// Package telemetry initializes OpenTelemetry without making observability a
// business dependency. When OTEL_EXPORTER_OTLP_ENDPOINT is empty, all calls are
// safe no-ops. Export failures are handled asynchronously by the SDK.
package telemetry

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Shutdown func(context.Context) error

const (
	StatusDisabled     = "disabled"
	StatusConfigured   = "configured"
	StatusUnavailable  = "unavailable"
	healthProbeEvery   = 5 * time.Second
	healthProbeTimeout = 2 * time.Second
	healthFailureLimit = 2
)

var runtimeStatus atomic.Value

func init() {
	runtimeStatus.Store(StatusDisabled)
}

// RuntimeStatus describes whether this process could initialize an OTLP
// exporter. It is intentionally a coarse health signal for workflow metadata;
// exporter delivery remains asynchronous and must not block business work.
func RuntimeStatus() string {
	value, _ := runtimeStatus.Load().(string)
	if value == "" {
		return StatusDisabled
	}
	return value
}

func Init(ctx context.Context, serviceName string) (Shutdown, error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))
	endpoint := strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
	if endpoint == "" {
		runtimeStatus.Store(StatusDisabled)
		initMetrics()
		return func(context.Context) error { return nil }, nil
	}
	endpoint = strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://")

	res, err := resource.New(ctx,
		resource.WithFromEnv(),
		resource.WithTelemetrySDK(),
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion(env("SERVICE_VERSION", "dev")),
			attribute.String("deployment.environment", env("DEPLOYMENT_ENVIRONMENT", "local")),
		),
	)
	if err != nil {
		runtimeStatus.Store(StatusUnavailable)
		return nil, err
	}

	traceOpts := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(endpoint)}
	metricOpts := []otlpmetricgrpc.Option{otlpmetricgrpc.WithEndpoint(endpoint)}
	if env("OTEL_EXPORTER_OTLP_INSECURE", "true") == "true" {
		traceOpts = append(traceOpts, otlptracegrpc.WithInsecure())
		metricOpts = append(metricOpts, otlpmetricgrpc.WithInsecure())
	}
	traceExporter, err := otlptracegrpc.New(ctx, traceOpts...)
	if err != nil {
		runtimeStatus.Store(StatusUnavailable)
		return nil, err
	}
	metricExporter, err := otlpmetricgrpc.New(ctx, metricOpts...)
	if err != nil {
		runtimeStatus.Store(StatusUnavailable)
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExporter),
		sdktrace.WithResource(res),
	)
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter,
			sdkmetric.WithInterval(15*time.Second))),
	)
	otel.SetTracerProvider(tp)
	otel.SetMeterProvider(mp)
	runtimeStatus.Store(StatusConfigured)
	initMetrics()
	monitorCtx, stopMonitor := context.WithCancel(ctx)
	go monitorEndpoint(monitorCtx, endpoint)

	return func(ctx context.Context) error {
		stopMonitor()
		return errors.Join(mp.Shutdown(ctx), tp.Shutdown(ctx))
	}, nil
}

// monitorEndpoint detects an exporter endpoint that becomes unreachable after
// startup. The probe is deliberately independent from SDK export queues: it
// only updates the diagnostic status used when a workflow run is created and
// never blocks business work.
func monitorEndpoint(ctx context.Context, endpoint string) {
	ticker := time.NewTicker(healthProbeEvery)
	defer ticker.Stop()
	failures := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			probeCtx, cancel := context.WithTimeout(ctx, healthProbeTimeout)
			err := probeEndpoint(probeCtx, endpoint)
			cancel()
			if err != nil {
				failures++
				if failures >= healthFailureLimit {
					runtimeStatus.Store(StatusUnavailable)
				}
				continue
			}
			failures = 0
			runtimeStatus.Store(StatusConfigured)
		}
	}
}

func probeEndpoint(ctx context.Context, endpoint string) error {
	conn, err := grpc.DialContext(ctx, endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return err
	}
	return conn.Close()
}

func env(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
