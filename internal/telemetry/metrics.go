package telemetry

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type instruments struct {
	jobsEnqueued         metric.Int64Counter
	schedulerTicks       metric.Int64Counter
	harvesterTicks       metric.Int64Counter
	stateTransitions     metric.Int64Counter
	apiErrors            metric.Int64Counter
	schedulerDuration    metric.Float64Histogram
	harvesterDuration    metric.Float64Histogram
	queuePublishDuration metric.Float64Histogram
	httpRequestDuration  metric.Float64Histogram
}

var (
	metricsMu  sync.RWMutex
	metricsSet *instruments
)

func initMetrics() {
	meter := otel.Meter("github.com/scholars-ai/scholar-core")
	jobs, _ := meter.Int64Counter("scholar_core_jobs_enqueued_total")
	schedulerTicks, _ := meter.Int64Counter("scholar_core_scheduler_ticks_total")
	harvesterTicks, _ := meter.Int64Counter("scholar_core_harvester_ticks_total")
	transitions, _ := meter.Int64Counter("scholar_core_state_transitions_total")
	apiErrors, _ := meter.Int64Counter("scholar_core_api_errors_total")
	schedulerDuration, _ := meter.Float64Histogram("scholar_core_scheduler_tick_duration_seconds")
	harvesterDuration, _ := meter.Float64Histogram("scholar_core_harvester_tick_duration_seconds")
	queueDuration, _ := meter.Float64Histogram("scholar_core_queue_publish_duration_seconds")
	httpDuration, _ := meter.Float64Histogram("scholar_core_http_request_duration_seconds")
	metricsMu.Lock()
	metricsSet = &instruments{
		jobsEnqueued: jobs, schedulerTicks: schedulerTicks, harvesterTicks: harvesterTicks,
		stateTransitions: transitions, apiErrors: apiErrors,
		schedulerDuration: schedulerDuration, harvesterDuration: harvesterDuration,
		queuePublishDuration: queueDuration, httpRequestDuration: httpDuration,
	}
	metricsMu.Unlock()
}

func currentMetrics() *instruments {
	metricsMu.RLock()
	defer metricsMu.RUnlock()
	return metricsSet
}

func RecordJobEnqueued(ctx context.Context, queue string, elapsed time.Duration) {
	if m := currentMetrics(); m != nil {
		attrs := metric.WithAttributes(attribute.String("queue", queue))
		m.jobsEnqueued.Add(ctx, 1, attrs)
		m.queuePublishDuration.Record(ctx, elapsed.Seconds(), attrs)
	}
}

func RecordSchedulerTick(ctx context.Context, elapsed time.Duration, status string) {
	if m := currentMetrics(); m != nil {
		attrs := metric.WithAttributes(attribute.String("status", status))
		m.schedulerTicks.Add(ctx, 1, attrs)
		m.schedulerDuration.Record(ctx, elapsed.Seconds(), attrs)
	}
}

func RecordHarvesterTick(ctx context.Context, elapsed time.Duration, status string) {
	if m := currentMetrics(); m != nil {
		attrs := metric.WithAttributes(attribute.String("status", status))
		m.harvesterTicks.Add(ctx, 1, attrs)
		m.harvesterDuration.Record(ctx, elapsed.Seconds(), attrs)
	}
}

func RecordTransition(ctx context.Context, from, to, trigger string) {
	if m := currentMetrics(); m != nil {
		m.stateTransitions.Add(ctx, 1, metric.WithAttributes(
			attribute.String("from_status", from),
			attribute.String("to_status", to),
			attribute.String("trigger_type", trigger),
		))
	}
}

func RecordAPIError(ctx context.Context, route, code string) {
	if m := currentMetrics(); m != nil {
		m.apiErrors.Add(ctx, 1, metric.WithAttributes(
			attribute.String("http_route", route), attribute.String("error_type", code),
		))
	}
}

func RecordHTTPRequest(ctx context.Context, route string, status int, elapsed time.Duration) {
	if m := currentMetrics(); m != nil {
		attrs := metric.WithAttributes(attribute.String("http_route", route))
		m.httpRequestDuration.Record(ctx, elapsed.Seconds(), attrs)
		if status >= 400 {
			family := "http_4xx"
			if status >= 500 {
				family = "http_5xx"
			}
			RecordAPIError(ctx, route, family)
		}
	}
}
