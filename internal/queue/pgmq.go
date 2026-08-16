// Package queue 是 pgmq 的薄封装（ADR-003）。
// 关键约定：Send 接收 pgx.Tx —— 入队必须与业务状态变更同事务提交，
// 保证"状态推进"与"任务投递"的原子性（SPEC-001 §3）。
package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/scholars-ai/scholar-core/internal/telemetry"
)

// Name 是 pgmq 队列名。合法值以 scholar-shared/schemas/queues.json 为准；
// 常量在此镜像声明，CI 后续增加与注册表的一致性校验（M1）。
type Name string

const (
	SourceFetch     Name = "source_fetch"
	TopicScout      Name = "topic_scout"
	TopicEvaluate   Name = "topic_evaluate"
	ArticleWrite    Name = "article_write"
	ArticleEvaluate Name = "article_evaluate"
	MemoryReflect   Name = "memory_reflect"
)

// Send 在事务内投递一条 job。payload 必须是 shared 契约中的 job 类型。
func Send(ctx context.Context, tx pgx.Tx, q Name, payload any, opts ...SendOption) (msgID int64, err error) {
	sent, err := SendMessage(ctx, tx, q, payload, opts...)
	return sent.MsgID, err
}

type TelemetryMeta struct {
	JobID         uuid.UUID  `json:"jobId"`
	CorrelationID uuid.UUID  `json:"correlationId"`
	ParentJobID   *uuid.UUID `json:"parentJobId,omitempty"`
	Traceparent   *string    `json:"traceparent,omitempty"`
	Tracestate    *string    `json:"tracestate,omitempty"`
	Baggage       *string    `json:"baggage,omitempty"`
	EnqueuedAt    time.Time  `json:"enqueuedAt"`
	TriggerType   string     `json:"triggerType"`
}

type SentMessage struct {
	MsgID int64
	Meta  TelemetryMeta
}

type SendOption func(*TelemetryMeta)

func WithCorrelation(id uuid.UUID) SendOption {
	return func(meta *TelemetryMeta) { meta.CorrelationID = id }
}

func WithParentJob(id uuid.UUID) SendOption {
	return func(meta *TelemetryMeta) { meta.ParentJobID = &id }
}

func WithTrigger(trigger string) SendOption {
	return func(meta *TelemetryMeta) { meta.TriggerType = trigger }
}

func SendMessage(
	ctx context.Context, tx pgx.Tx, q Name, payload any, opts ...SendOption,
) (sent SentMessage, err error) {
	started := time.Now()
	ctx, span := otel.Tracer("scholar-core/queue").Start(ctx, "messaging.publish",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "pgmq"),
			attribute.String(telemetry.AttrQueue, string(q)),
		),
	)
	defer func() {
		telemetry.RecordJobEnqueued(ctx, string(q), time.Since(started))
		if err != nil {
			telemetry.MarkError(span, err, "queue publish failed")
		}
		span.End()
	}()

	meta := TelemetryMeta{
		JobID: uuid.New(), CorrelationID: uuid.New(), EnqueuedAt: time.Now().UTC(),
		TriggerType: "api",
	}
	for _, opt := range opts {
		opt(&meta)
	}
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	meta.Traceparent = optionalString(carrier.Get("traceparent"))
	meta.Tracestate = optionalString(carrier.Get("tracestate"))
	meta.Baggage = optionalString(carrier.Get("baggage"))

	body, err := payloadWithMeta(payload, meta)
	if err != nil {
		return SentMessage{}, fmt.Errorf("queue send %s: marshal payload: %w", q, err)
	}
	if err := tx.QueryRow(ctx, "select pgmq.send($1, $2::jsonb)", string(q), body).Scan(&sent.MsgID); err != nil {
		return SentMessage{}, fmt.Errorf("queue send %s: %w", q, err)
	}
	sent.Meta = meta
	span.SetAttributes(
		attribute.Int64("messaging.message.id", sent.MsgID),
		attribute.String(telemetry.AttrJobID, meta.JobID.String()),
		attribute.String(telemetry.AttrCorrelationID, meta.CorrelationID.String()),
	)
	return sent, nil
}

func payloadWithMeta(payload any, meta TelemetryMeta) ([]byte, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	var object map[string]any
	if err := json.Unmarshal(raw, &object); err != nil {
		return nil, err
	}
	if object == nil {
		return nil, fmt.Errorf("payload must be a JSON object")
	}
	object["_meta"] = meta
	return json.Marshal(object)
}

func optionalString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}
