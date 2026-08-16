package telemetry

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// RegisterDBPoolMetrics exposes only aggregate pool counts. The callback reads
// pgxpool's in-memory stats and therefore never touches the business database.
func RegisterDBPoolMetrics(pool *pgxpool.Pool) error {
	meter := otel.Meter("github.com/scholars-ai/scholar-core/dbpool")
	acquired, err := meter.Int64ObservableGauge("scholar_core_db_pool_acquired")
	if err != nil {
		return err
	}
	idle, err := meter.Int64ObservableGauge("scholar_core_db_pool_idle")
	if err != nil {
		return err
	}
	maximum, err := meter.Int64ObservableGauge("scholar_core_db_pool_max")
	if err != nil {
		return err
	}
	_, err = meter.RegisterCallback(func(_ context.Context, observer metric.Observer) error {
		stats := pool.Stat()
		observer.ObserveInt64(acquired, int64(stats.AcquiredConns()))
		observer.ObserveInt64(idle, int64(stats.IdleConns()))
		observer.ObserveInt64(maximum, int64(stats.MaxConns()))
		return nil
	}, acquired, idle, maximum)
	return err
}
