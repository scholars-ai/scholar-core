// Package harvester 收割 agents 写入的结果并推进状态机（唯一写入口原则）。
//
// 两个职责（SPEC-008 §3）：
//  1. 新 candidate → **事件驱动**投递 topic_evaluate（不等固定时刻，纪律 2）
//  2. 已有评分的 candidate → 推进 candidate→scored / <60 自动 rejected
//
// 轮询间隔 15s：candidate 产生到评分入队 < 1 分钟的验收要求（SPEC-008 §6）。
// 防重投复用 schedule_runs 唯一约束（key = topic_evaluate:<topic_id>）。
package harvester

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/scholars-ai/scholar-core/internal/db/dbgen"
	"github.com/scholars-ai/scholar-core/internal/pipeline"
	"github.com/scholars-ai/scholar-core/internal/queue"
	"github.com/scholars-ai/scholar-core/internal/telemetry"
)

// 分档阈值（SPEC-004 §2）：>=75 推荐、60–75 备选、<60 自动 rejected。
// 推荐/备选都推进到 scored（同一状态，分数区分展示），<60 直接 rejected。
const autoRejectBelow = 60.0

type Harvester struct {
	pool *pgxpool.Pool
	q    *dbgen.Queries
	log  *slog.Logger
	now  func() time.Time
}

func New(pool *pgxpool.Pool, log *slog.Logger) *Harvester {
	return &Harvester{pool: pool, q: dbgen.New(pool), log: log, now: time.Now}
}

func (h *Harvester) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 15 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	h.log.Info("harvester started", "interval", interval.String())
	for {
		select {
		case <-ctx.Done():
			h.log.Info("harvester stopped")
			return
		case <-ticker.C:
			h.Tick(ctx)
		}
	}
}

// Tick 一轮收割。导出以便测试。
func (h *Harvester) Tick(ctx context.Context) {
	started := time.Now()
	ctx, span := otel.Tracer("scholar-core/harvester").Start(ctx, "harvester.tick")
	defer func() {
		telemetry.RecordHarvesterTick(ctx, time.Since(started), "ok")
		span.End()
	}()
	h.enqueueEvaluations(ctx)
	h.transitionScored(ctx)
}

// enqueueEvaluations：为尚无评分任务的 candidate 投递 topic_evaluate。
func (h *Harvester) enqueueEvaluations(ctx context.Context) {
	ctx, span := otel.Tracer("scholar-core/harvester").Start(ctx, "harvester.scan_candidates")
	defer span.End()
	candidates, err := h.q.PendingCandidates(ctx, 50)
	if err != nil {
		h.log.Error("pending candidates query failed", "error", err)
		return
	}
	for _, c := range candidates {
		if err := h.enqueueOne(ctx, c.ID, c.CorrelationID); err != nil {
			h.log.Warn("enqueue topic_evaluate failed", "topic", c.Title, "error", err)
		}
	}
}

func (h *Harvester) enqueueOne(ctx context.Context, topicID uuid.UUID, correlationID uuid.NullUUID) error {
	ctx, span := otel.Tracer("scholar-core/harvester").Start(ctx, "harvester.enqueue_topic_evaluate")
	span.SetAttributes(attribute.String(telemetry.AttrTopicID, topicID.String()))
	defer span.End()
	// planned_at 固定为零值时刻：每个 topic 只允许投递一次评分任务
	// （重评走人工触发，M2 再考虑自动重评策略）。
	epoch := time.Unix(0, 0).UTC()
	return pgx.BeginFunc(ctx, h.pool, func(tx pgx.Tx) error {
		qtx := h.q.WithTx(tx)
		runID, err := qtx.RecordScheduleRun(ctx, dbgen.RecordScheduleRunParams{
			ScheduleKey: "topic_evaluate:" + topicID.String(),
			PlannedAt:   pgtype.Timestamptz{Time: epoch, Valid: true},
			Queue:       string(queue.TopicEvaluate),
		})
		if errors.Is(err, pgx.ErrNoRows) {
			return nil // 已投递过
		}
		if err != nil {
			return err
		}
		opts := []queue.SendOption{queue.WithTrigger("harvester")}
		if correlationID.Valid {
			opts = append(opts, queue.WithCorrelation(correlationID.UUID))
		}
		msgID, err := queue.Send(ctx, tx, queue.TopicEvaluate, map[string]string{
			"topicId": topicID.String(),
		}, opts...)
		if err != nil {
			return err
		}
		return qtx.UpdateScheduleRunMsgID(ctx, dbgen.UpdateScheduleRunMsgIDParams{
			MsgID: pgtype.Int8{Int64: msgID, Valid: true},
			ID:    runID,
		})
	})
}

// transitionScored：评分已写回的 candidate → scored / rejected。
func (h *Harvester) transitionScored(ctx context.Context) {
	ctx, span := otel.Tracer("scholar-core/harvester").Start(ctx, "harvester.scan_evaluations")
	defer span.End()
	rows, err := h.q.ScoredTopicsWithoutTransition(ctx, 50)
	if err != nil {
		h.log.Error("scored topics query failed", "error", err)
		return
	}
	for _, row := range rows {
		transitionCtx, transitionSpan := otel.Tracer("scholar-core/pipeline").Start(
			ctx, "pipeline.transition_topic",
		)
		transitionSpan.SetAttributes(
			attribute.String(telemetry.AttrTopicID, row.ID.String()),
			attribute.String(telemetry.AttrFromStatus, string(dbgen.TopicStatusCandidate)),
			attribute.String(telemetry.AttrTriggerType, "harvester"),
		)
		score, err := numericFloat(row.TotalScore)
		if err != nil {
			telemetry.MarkError(transitionSpan, err, "topic transition failed")
			transitionSpan.End()
			h.log.Error("bad total_score", "topic", row.ID, "error", err)
			continue
		}
		to := dbgen.TopicStatusScored
		if score < autoRejectBelow {
			to = dbgen.TopicStatusRejected
		}
		if !pipeline.CanTopicTransition(dbgen.TopicStatusCandidate, to) {
			transitionSpan.SetStatus(codes.Error, "illegal transition blocked")
			transitionSpan.End()
			h.log.Error("illegal transition blocked", "from", "candidate", "to", to)
			continue
		}
		transitionSpan.SetAttributes(attribute.String(telemetry.AttrToStatus, string(to)))
		_, err = h.q.TransitionTopic(transitionCtx, dbgen.TransitionTopicParams{
			TopicID:     row.ID,
			ToStatus:    to,
			FromStatus:  dbgen.TopicStatusCandidate,
			Score:       row.TotalScore,
			ActorType:   "system",
			TriggerType: "harvester",
			TriggerID:   pgtype.Text{String: row.EvaluationID.String(), Valid: true},
			Reason: pgtype.Text{
				String: transitionReason(to, score), Valid: true,
			},
			CorrelationID: row.CorrelationID,
		})
		if errors.Is(err, pgx.ErrNoRows) {
			transitionSpan.End()
			continue // 并发下已被处理，幂等
		}
		if err != nil {
			telemetry.MarkError(transitionSpan, err, "topic transition failed")
			transitionSpan.End()
			h.log.Error("transition failed", "topic", row.ID, "error", err)
			continue
		}
		telemetry.RecordTransition(transitionCtx, string(dbgen.TopicStatusCandidate), string(to), "harvester")
		transitionSpan.End()
		h.log.Info("topic transitioned", "topic", row.ID, "to", to, "score", score)
	}
}

func transitionReason(to dbgen.TopicStatus, score float64) string {
	if to == dbgen.TopicStatusRejected {
		return fmt.Sprintf("automatic rejection: score %.2f below %.2f", score, autoRejectBelow)
	}
	return fmt.Sprintf("evaluation completed: score %.2f", score)
}

func numericFloat(n pgtype.Numeric) (float64, error) {
	v, err := n.Float64Value()
	if err != nil {
		return 0, err
	}
	if !v.Valid {
		return 0, errors.New("null total_score")
	}
	return v.Float64, nil
}
