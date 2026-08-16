// Package harvester 收割 agents 写入的结果并推进状态机（唯一写入口原则）。
//
// M1/M2 职责：
//  1. 新 candidate → **事件驱动**投递 topic_evaluate（不等固定时刻，纪律 2）
//  2. 已有评分的 candidate → 推进 candidate→scored / <60 自动 rejected
//  3. approved → 按 target_platforms 原子投递 article_write 并推进 in_writing
//  4. Article(draft) → 投递 article_evaluate；全平台文章齐备后 Topic → written
//  5. ArticleJudge 结果 → draft→scored→pending_review / rewrite_queued
//  6. 回炉时原子投递下一不可变版本的 article_write，最多生成 v3
//
// 轮询间隔 15s：candidate 产生到评分入队 < 1 分钟的验收要求（SPEC-008 §6）。
// 防重投复用 schedule_runs 唯一约束（key = topic_evaluate:<topic_id>）。
package harvester

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
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

const (
	maxArticleVersion = int32(3)
	redoOutlineBelow  = 6.0
)

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
	h.enqueueWriting(ctx)
	h.enqueueArticleEvaluations(ctx)
	h.transitionArticleEvaluations(ctx)
	h.decideScoredArticles(ctx)
	h.transitionWritten(ctx)
}

// enqueueWriting：按平台分派 approved Topic。所有入队与 approved→in_writing 审计同事务。
func (h *Harvester) enqueueWriting(ctx context.Context) {
	ctx, span := otel.Tracer("scholar-core/harvester").Start(ctx, "harvester.scan_approved_topics")
	defer span.End()
	topics, err := h.q.PendingApprovedTopics(ctx, 50)
	if err != nil {
		h.log.Error("pending approved topics query failed", "error", err)
		return
	}
	for _, topic := range topics {
		if err := h.enqueueTopicWriting(ctx, topic.ID, topic.TargetPlatforms, topic.CorrelationID); err != nil {
			h.log.Warn("enqueue article_write failed", "topic", topic.Title, "error", err)
		}
	}
}

func (h *Harvester) enqueueTopicWriting(
	ctx context.Context,
	topicID uuid.UUID,
	platforms []dbgen.Platform,
	correlationID uuid.NullUUID,
) error {
	ctx, span := otel.Tracer("scholar-core/harvester").Start(ctx, "harvester.enqueue_article_write")
	span.SetAttributes(attribute.String(telemetry.AttrTopicID, topicID.String()))
	defer span.End()
	if len(platforms) == 0 {
		return fmt.Errorf("topic %s has no target platforms", topicID)
	}
	epoch := time.Unix(0, 0).UTC()
	return pgx.BeginFunc(ctx, h.pool, func(tx pgx.Tx) error {
		qtx := h.q.WithTx(tx)
		for _, platform := range platforms {
			runID, err := qtx.RecordScheduleRun(ctx, dbgen.RecordScheduleRunParams{
				ScheduleKey: articleWriteScheduleKey(topicID, platform),
				PlannedAt:   pgtype.Timestamptz{Time: epoch, Valid: true},
				Queue:       string(queue.ArticleWrite),
			})
			if errors.Is(err, pgx.ErrNoRows) {
				continue
			}
			if err != nil {
				return err
			}
			opts := []queue.SendOption{queue.WithTrigger("harvester")}
			if correlationID.Valid {
				opts = append(opts, queue.WithCorrelation(correlationID.UUID))
			}
			msgID, err := queue.Send(ctx, tx, queue.ArticleWrite, map[string]string{
				"topicId":  topicID.String(),
				"platform": string(platform),
			}, opts...)
			if err != nil {
				return err
			}
			if err := qtx.UpdateScheduleRunMsgID(ctx, dbgen.UpdateScheduleRunMsgIDParams{
				MsgID: pgtype.Int8{Int64: msgID, Valid: true}, ID: runID,
			}); err != nil {
				return err
			}
		}
		if !pipeline.CanTopicTransition(dbgen.TopicStatusApproved, dbgen.TopicStatusInWriting) {
			return pipeline.ErrInvalidTransition("topic", "approved", "in_writing")
		}
		_, err := qtx.TransitionTopic(ctx, dbgen.TransitionTopicParams{
			TopicID: topicID, FromStatus: dbgen.TopicStatusApproved,
			ToStatus:  dbgen.TopicStatusInWriting,
			ActorType: "system", TriggerType: "harvester",
			Reason:        pgtype.Text{String: "writing jobs dispatched for all target platforms", Valid: true},
			CorrelationID: correlationID,
		})
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return err
	})
}

func articleWriteScheduleKey(topicID uuid.UUID, platform dbgen.Platform) string {
	return fmt.Sprintf("article_write:%s:%s", topicID, platform)
}

// enqueueArticleEvaluations：Agents 只写 Article，评分任务仍由 Core 投递。
func (h *Harvester) enqueueArticleEvaluations(ctx context.Context) {
	ctx, span := otel.Tracer("scholar-core/harvester").Start(ctx, "harvester.scan_draft_articles")
	defer span.End()
	articles, err := h.q.DraftArticlesPendingEvaluation(ctx, 100)
	if err != nil {
		h.log.Error("draft articles query failed", "error", err)
		return
	}
	for _, article := range articles {
		if err := h.enqueueArticleEvaluation(ctx, article.ID, article.CorrelationID); err != nil {
			h.log.Warn("enqueue article_evaluate failed", "article", article.ID, "error", err)
		}
	}
}

func (h *Harvester) enqueueArticleEvaluation(
	ctx context.Context, articleID uuid.UUID, correlationID uuid.NullUUID,
) error {
	epoch := time.Unix(0, 0).UTC()
	return pgx.BeginFunc(ctx, h.pool, func(tx pgx.Tx) error {
		qtx := h.q.WithTx(tx)
		runID, err := qtx.RecordScheduleRun(ctx, dbgen.RecordScheduleRunParams{
			ScheduleKey: "article_evaluate:" + articleID.String(),
			PlannedAt:   pgtype.Timestamptz{Time: epoch, Valid: true},
			Queue:       string(queue.ArticleEvaluate),
		})
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		opts := []queue.SendOption{queue.WithTrigger("harvester")}
		if correlationID.Valid {
			opts = append(opts, queue.WithCorrelation(correlationID.UUID))
		}
		msgID, err := queue.Send(ctx, tx, queue.ArticleEvaluate, map[string]string{
			"articleId": articleID.String(),
		}, opts...)
		if err != nil {
			return err
		}
		return qtx.UpdateScheduleRunMsgID(ctx, dbgen.UpdateScheduleRunMsgIDParams{
			MsgID: pgtype.Int8{Int64: msgID, Valid: true}, ID: runID,
		})
	})
}

// transitionArticleEvaluations 把 Agents 追加的评分结果收割为显式 scored 审计节点。
func (h *Harvester) transitionArticleEvaluations(ctx context.Context) {
	rows, err := h.q.ArticleEvaluationsWithoutTransition(ctx, 100)
	if err != nil {
		h.log.Error("article evaluations query failed", "error", err)
		return
	}
	for _, row := range rows {
		if !pipeline.CanArticleTransition(dbgen.ArticleStatusDraft, dbgen.ArticleStatusScored) {
			h.log.Error("illegal article transition blocked", "from", "draft", "to", "scored")
			return
		}
		_, err := h.q.TransitionArticle(ctx, dbgen.TransitionArticleParams{
			ArticleID: row.ID, FromStatus: dbgen.ArticleStatusDraft,
			ToStatus: dbgen.ArticleStatusScored, Score: row.TotalScore,
			ActorType: "system", TriggerType: "harvester",
			TriggerID:     pgtype.Text{String: row.EvaluationID.String(), Valid: true},
			Reason:        pgtype.Text{String: "article evaluation completed", Valid: true},
			CorrelationID: row.CorrelationID,
		})
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			h.log.Error("article scored transition failed", "article", row.ID, "error", err)
			continue
		}
		telemetry.RecordTransition(ctx, string(dbgen.ArticleStatusDraft), string(dbgen.ArticleStatusScored), "harvester")
		h.log.Info("article scored", "article", row.ID, "version", row.Version)
	}
}

// decideScoredArticles 使用 ArticleJudge 已固化的 passed 判定决定人工终审或回炉。
func (h *Harvester) decideScoredArticles(ctx context.Context) {
	rows, err := h.q.ScoredArticlesWithoutDecision(ctx, 100)
	if err != nil {
		h.log.Error("scored articles query failed", "error", err)
		return
	}
	for _, row := range rows {
		if row.Passed || row.Version >= maxArticleVersion {
			if err := h.moveArticleToPendingReview(ctx, row); err != nil {
				h.log.Error("article pending-review transition failed", "article", row.ID, "error", err)
			}
			continue
		}
		if err := h.enqueueArticleRewrite(ctx, row); err != nil {
			h.log.Error("article rewrite dispatch failed", "article", row.ID, "error", err)
		}
	}
}

func (h *Harvester) moveArticleToPendingReview(
	ctx context.Context, row dbgen.ScoredArticlesWithoutDecisionRow,
) error {
	if !pipeline.CanArticleTransition(dbgen.ArticleStatusScored, dbgen.ArticleStatusPendingReview) {
		return pipeline.ErrInvalidTransition("article", "scored", "pending_review")
	}
	score, err := numericFloat(row.TotalScore)
	if err != nil {
		return err
	}
	reason := fmt.Sprintf("article evaluation passed: score %.2f", score)
	metadata := map[string]any{"evaluationId": row.EvaluationID, "passed": row.Passed}
	if !row.Passed {
		reason = fmt.Sprintf("rewrite limit reached at v%d; manual review required", row.Version)
		metadata["rewriteLimitReached"] = true
	}
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	_, err = h.q.TransitionArticle(ctx, dbgen.TransitionArticleParams{
		ArticleID: row.ID, FromStatus: dbgen.ArticleStatusScored,
		ToStatus: dbgen.ArticleStatusPendingReview, Score: row.TotalScore,
		ActorType: "system", TriggerType: "harvester",
		TriggerID:     pgtype.Text{String: row.EvaluationID.String(), Valid: true},
		Reason:        pgtype.Text{String: reason, Valid: true},
		CorrelationID: row.CorrelationID,
		Metadata:      metadataJSON,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err == nil {
		telemetry.RecordTransition(ctx, string(dbgen.ArticleStatusScored), string(dbgen.ArticleStatusPendingReview), "harvester")
	}
	return err
}

func (h *Harvester) enqueueArticleRewrite(
	ctx context.Context, row dbgen.ScoredArticlesWithoutDecisionRow,
) error {
	if !pipeline.CanArticleTransition(dbgen.ArticleStatusScored, dbgen.ArticleStatusRewriteQueued) {
		return pipeline.ErrInvalidTransition("article", "scored", "rewrite_queued")
	}
	feedback, redoOutline, err := articleRewriteFeedback(row)
	if err != nil {
		return err
	}
	nextVersion := row.Version + 1
	epoch := time.Unix(0, 0).UTC()
	return pgx.BeginFunc(ctx, h.pool, func(tx pgx.Tx) error {
		qtx := h.q.WithTx(tx)
		metadata, err := json.Marshal(map[string]any{
			"evaluationId": row.EvaluationID,
			"nextVersion":  nextVersion,
			"redoOutline":  redoOutline,
		})
		if err != nil {
			return err
		}
		_, err = qtx.TransitionArticle(ctx, dbgen.TransitionArticleParams{
			ArticleID: row.ID, FromStatus: dbgen.ArticleStatusScored,
			ToStatus: dbgen.ArticleStatusRewriteQueued, Score: row.TotalScore,
			ActorType: "system", TriggerType: "harvester",
			TriggerID:     pgtype.Text{String: row.EvaluationID.String(), Valid: true},
			Reason:        pgtype.Text{String: "article evaluation failed; rewrite dispatched", Valid: true},
			CorrelationID: row.CorrelationID,
			Metadata:      metadata,
		})
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		runID, err := qtx.RecordScheduleRun(ctx, dbgen.RecordScheduleRunParams{
			ScheduleKey: articleRewriteScheduleKey(row.TopicID, row.Platform, nextVersion),
			PlannedAt:   pgtype.Timestamptz{Time: epoch, Valid: true},
			Queue:       string(queue.ArticleWrite),
		})
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("rewrite schedule already exists for article %s v%d", row.ID, nextVersion)
		}
		if err != nil {
			return err
		}
		opts := []queue.SendOption{queue.WithTrigger("harvester")}
		if row.CorrelationID.Valid {
			opts = append(opts, queue.WithCorrelation(row.CorrelationID.UUID))
		}
		msgID, err := queue.Send(ctx, tx, queue.ArticleWrite, map[string]any{
			"topicId":  row.TopicID.String(),
			"platform": string(row.Platform),
			"rewrite": map[string]any{
				"previousArticleId":  row.ID.String(),
				"evaluationFeedback": feedback,
				"redoOutline":        redoOutline,
			},
		}, opts...)
		if err != nil {
			return err
		}
		if err := qtx.UpdateScheduleRunMsgID(ctx, dbgen.UpdateScheduleRunMsgIDParams{
			MsgID: pgtype.Int8{Int64: msgID, Valid: true}, ID: runID,
		}); err != nil {
			return err
		}
		telemetry.RecordTransition(ctx, string(dbgen.ArticleStatusScored), string(dbgen.ArticleStatusRewriteQueued), "harvester")
		return nil
	})
}

func articleRewriteScheduleKey(topicID uuid.UUID, platform dbgen.Platform, version int32) string {
	return fmt.Sprintf("article_write:%s:%s:rewrite:%d", topicID, platform, version)
}

func articleRewriteFeedback(row dbgen.ScoredArticlesWithoutDecisionRow) (string, bool, error) {
	var scores map[string]float64
	if err := json.Unmarshal(row.DimensionScores, &scores); err != nil {
		return "", false, fmt.Errorf("decode dimension scores: %w", err)
	}
	var reasons map[string]string
	if err := json.Unmarshal(row.DimensionReasons, &reasons); err != nil {
		return "", false, fmt.Errorf("decode dimension reasons: %w", err)
	}
	keys := make([]string, 0, len(scores))
	for key := range scores {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var feedback strings.Builder
	fmt.Fprintf(&feedback, "总体评审：%s\n维度反馈：", row.Rationale)
	for _, key := range keys {
		fmt.Fprintf(&feedback, "\n- %s: %.1f/10", key, scores[key])
		if reason := strings.TrimSpace(reasons[key]); reason != "" {
			fmt.Fprintf(&feedback, " — %s", reason)
		}
	}
	if row.VetoedDimension.Valid {
		fmt.Fprintf(&feedback, "\n一票否决维度：%s", row.VetoedDimension.String)
	}
	return feedback.String(), scores["structure"] < redoOutlineBelow, nil
}

func (h *Harvester) transitionWritten(ctx context.Context) {
	rows, err := h.q.InWritingTopicsReady(ctx, 50)
	if err != nil {
		h.log.Error("in-writing topics query failed", "error", err)
		return
	}
	for _, row := range rows {
		if !pipeline.CanTopicTransition(dbgen.TopicStatusInWriting, dbgen.TopicStatusWritten) {
			h.log.Error("illegal transition blocked", "from", "in_writing", "to", "written")
			return
		}
		_, err := h.q.TransitionTopic(ctx, dbgen.TransitionTopicParams{
			TopicID: row.ID, FromStatus: dbgen.TopicStatusInWriting,
			ToStatus:  dbgen.TopicStatusWritten,
			ActorType: "system", TriggerType: "harvester",
			Reason:        pgtype.Text{String: "all target platform articles created", Valid: true},
			CorrelationID: row.CorrelationID,
		})
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			h.log.Error("written transition failed", "topic", row.ID, "error", err)
			continue
		}
		telemetry.RecordTransition(ctx, string(dbgen.TopicStatusInWriting), string(dbgen.TopicStatusWritten), "harvester")
		h.log.Info("topic writing completed", "topic", row.ID)
	}
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
