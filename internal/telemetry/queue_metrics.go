package telemetry

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

func RegisterQueueMetrics(pool *pgxpool.Pool) error {
	meter := otel.Meter("github.com/scholars-ai/scholar-core/pgmq")
	visible, err := meter.Int64ObservableGauge("scholar_pgmq_visible_messages")
	if err != nil {
		return err
	}
	inFlight, err := meter.Int64ObservableGauge("scholar_pgmq_in_flight_messages")
	if err != nil {
		return err
	}
	current, err := meter.Int64ObservableGauge("scholar_pgmq_current_messages")
	if err != nil {
		return err
	}
	total, err := meter.Int64ObservableGauge("scholar_pgmq_total_messages")
	if err != nil {
		return err
	}
	oldest, err := meter.Int64ObservableGauge("scholar_pgmq_oldest_visible_age_seconds")
	if err != nil {
		return err
	}
	_, err = meter.RegisterCallback(func(ctx context.Context, observer metric.Observer) error {
		queryCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		rows, err := pool.Query(queryCtx, `
			select queue_name, queue_length, total_messages, queue_visible_length,
			       coalesce(oldest_msg_age_sec, 0)
			from pgmq.metrics_all()
		`)
		if err != nil {
			return nil // 观测查询失败不得影响业务或导出循环
		}
		defer rows.Close()
		for rows.Next() {
			var queue string
			var queueLength, totalMessages, visibleMessages, oldestAge int64
			if err := rows.Scan(
				&queue, &queueLength, &totalMessages, &visibleMessages, &oldestAge,
			); err != nil {
				continue
			}
			waitingMessages, inFlightMessages, currentMessages := queueDepths(
				queueLength, visibleMessages,
			)
			attrs := metric.WithAttributes(attribute.String("queue", queue))
			observer.ObserveInt64(visible, waitingMessages, attrs)
			observer.ObserveInt64(inFlight, inFlightMessages, attrs)
			observer.ObserveInt64(current, currentMessages, attrs)
			observer.ObserveInt64(total, totalMessages, attrs)
			observer.ObserveInt64(oldest, oldestAge, attrs)
		}
		return nil
	}, visible, inFlight, current, total, oldest)
	return err
}

func queueDepths(queueLength, visibleMessages int64) (waiting, inFlight, current int64) {
	current = max(queueLength, 0)
	waiting = min(max(visibleMessages, 0), current)
	inFlight = current - waiting
	return waiting, inFlight, current
}
