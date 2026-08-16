// Package scheduler 实现 SPEC-008 §3.1 的动态调度：
// 唯一写死的是 tick 粒度（1 分钟）——它是内部检查频率；一切业务频率来自 DB。
//
// 正确性设计：
//   - 防重投：schedule_runs(schedule_key, planned_at) 唯一约束，投递前先 INSERT，
//     冲突即本窗口已投递过（多实例安全，无需额外锁）。
//   - 入队与留痕/健康状态更新同事务（ADR-003 的事务性入队纪律）。
//   - 单条配置非法只跳过并告警，不崩整个 tick。
package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"

	"github.com/scholars-ai/scholar-core/internal/db/dbgen"
	"github.com/scholars-ai/scholar-core/internal/queue"
	"github.com/scholars-ai/scholar-core/internal/telemetry"
)

// Settings 镜像 shared 的 SchedulerSettings schema（jsonb 整体存取，API 层已校验）。
type Settings struct {
	SourceFetch struct {
		Enabled                bool `json:"enabled"`
		DefaultIntervalMinutes int  `json:"defaultIntervalMinutes"`
	} `json:"sourceFetch"`
	TopicScout struct {
		Enabled     bool     `json:"enabled"`
		Times       []string `json:"times"` // "HH:MM"
		Timezone    string   `json:"timezone"`
		MinNewItems int      `json:"minNewItems"`
	} `json:"topicScout"`
	TopicEvaluate struct {
		Enabled        bool `json:"enabled"`
		MaxConcurrency int  `json:"maxConcurrency"`
	} `json:"topicEvaluate"`
}

const scheduledScoutMaxItems = 20

// DefaultSettings 仅用于首次 seed（SPEC-008 §3.2）。
func DefaultSettings() Settings {
	var s Settings
	s.SourceFetch.Enabled = true
	s.SourceFetch.DefaultIntervalMinutes = 60
	s.TopicScout.Enabled = true
	s.TopicScout.Times = []string{"08:00", "20:00"}
	s.TopicScout.Timezone = "Asia/Shanghai"
	s.TopicScout.MinNewItems = 5
	s.TopicEvaluate.Enabled = true
	s.TopicEvaluate.MaxConcurrency = 2
	return s
}

type Scheduler struct {
	pool *pgxpool.Pool
	q    *dbgen.Queries
	log  *slog.Logger
	// now 可注入以便测试
	now func() time.Time
}

func New(pool *pgxpool.Pool, log *slog.Logger) *Scheduler {
	return &Scheduler{pool: pool, q: dbgen.New(pool), log: log, now: time.Now}
}

// Seed 在 scheduler_settings 为空时写入默认值。
func (s *Scheduler) Seed(ctx context.Context) error {
	raw, err := json.Marshal(DefaultSettings())
	if err != nil {
		return err
	}
	return s.q.SeedSchedulerSettings(ctx, raw)
}

// Run 启动 1 分钟 tick 循环，直到 ctx 取消。
func (s *Scheduler) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	s.log.Info("scheduler started", "tick", "1m")
	for {
		select {
		case <-ctx.Done():
			s.log.Info("scheduler stopped")
			return
		case <-ticker.C:
			if err := s.Tick(ctx); err != nil {
				// tick 级错误只告警：下一分钟会重试，且不影响 API 服务
				s.log.Error("scheduler tick failed", "error", err)
			}
		}
	}
}

// Tick 执行一轮调度检查。导出以便测试与手动触发。

func (s *Scheduler) Tick(ctx context.Context) (err error) {
	started := time.Now()
	ctx, span := otel.Tracer("scholar-core/scheduler").Start(ctx, "scheduler.tick")
	defer func() {
		status := "ok"
		if err != nil {
			status = "error"
			telemetry.MarkError(span, err, "scheduler tick failed")
		}
		telemetry.RecordSchedulerTick(ctx, time.Since(started), status)
		span.End()
	}()
	settings, err := s.loadSettings(ctx)
	if err != nil {
		return fmt.Errorf("load settings: %w", err)
	}
	if settings.SourceFetch.Enabled {
		s.tickSourceFetch(ctx, settings)
	}
	if settings.TopicScout.Enabled {
		s.tickTopicScout(ctx, settings)
	}
	return nil
}

func (s *Scheduler) loadSettings(ctx context.Context) (Settings, error) {
	ctx, span := otel.Tracer("scholar-core/scheduler").Start(ctx, "scheduler.load_settings")
	defer span.End()
	row, err := s.q.GetSchedulerSettings(ctx)
	if err != nil {
		return Settings{}, err
	}
	var out Settings
	if err := json.Unmarshal(row.Settings, &out); err != nil {
		return Settings{}, fmt.Errorf("corrupt scheduler_settings: %w", err)
	}
	return out, nil
}

// tickSourceFetch：到期的源投递 source_fetch。
// 每个源独立事务：一个源失败不影响其他源（SPEC-008 §6 的隔离原则）。
func (s *Scheduler) tickSourceFetch(ctx context.Context, settings Settings) {
	ctx, span := otel.Tracer("scholar-core/scheduler").Start(ctx, "scheduler.find_due_sources")
	defer span.End()
	due, err := s.q.DueSources(ctx)
	if err != nil {
		s.log.Error("due sources query failed", "error", err)
		return
	}
	for _, src := range due {
		interval := time.Duration(settings.SourceFetch.DefaultIntervalMinutes) * time.Minute
		if v := sourceInterval(src.FetchConfig); v > 0 {
			interval = v
		}
		if err := s.enqueueSourceFetch(ctx, src.ID, interval); err != nil {
			s.log.Warn("enqueue source_fetch failed", "source", src.Name, "error", err)
		}
	}
}

// sourceInterval 从 fetch_config 读 intervalMinutes 覆盖；非法值返回 0（用全局默认）。
func sourceInterval(raw []byte) time.Duration {
	var cfg struct {
		IntervalMinutes *float64 `json:"intervalMinutes"`
	}
	if err := json.Unmarshal(raw, &cfg); err != nil || cfg.IntervalMinutes == nil {
		return 0
	}
	m := int(*cfg.IntervalMinutes)
	if m < 5 || m > 10080 { // 与 schema 的边界一致
		return 0
	}
	return time.Duration(m) * time.Minute
}

func (s *Scheduler) enqueueSourceFetch(ctx context.Context, sourceID uuid.UUID, interval time.Duration) error {
	ctx, span := otel.Tracer("scholar-core/scheduler").Start(ctx, "scheduler.enqueue_source_fetch")
	defer span.End()
	now := s.now().UTC()
	planned := now.Truncate(time.Minute)
	key := "source_fetch:" + sourceID.String()

	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		qtx := s.q.WithTx(tx)
		// 防重投登记：冲突 = 本窗口已投递（并发实例或重复 tick）
		runID, err := qtx.RecordScheduleRun(ctx, dbgen.RecordScheduleRunParams{
			ScheduleKey: key,
			PlannedAt:   pgtype.Timestamptz{Time: planned, Valid: true},
			Queue:       string(queue.SourceFetch),
		})
		if isNoRows(err) { // on conflict do nothing → 本窗口已投递（并发实例或重复 tick）
			return nil
		}
		if err != nil {
			return err
		}
		msgID, err := queue.Send(ctx, tx, queue.SourceFetch, map[string]string{
			"sourceId": sourceID.String(),
		}, queue.WithTrigger("scheduler"))
		if err != nil {
			return err
		}
		if err := qtx.MarkSourceScheduled(ctx, dbgen.MarkSourceScheduledParams{
			SourceID:  sourceID,
			NextRunAt: pgtype.Timestamptz{Time: now.Add(interval), Valid: true},
		}); err != nil {
			return err
		}
		return setScheduleRunMsg(ctx, tx, runID, msgID)
	})
}

// tickTopicScout：当前时刻（分钟粒度）命中配置的每日时刻则投递。
func (s *Scheduler) tickTopicScout(ctx context.Context, settings Settings) {
	ctx, span := otel.Tracer("scholar-core/scheduler").Start(ctx, "scheduler.enqueue_topic_scout")
	defer span.End()
	loc, err := time.LoadLocation(settings.TopicScout.Timezone)
	if err != nil {
		s.log.Warn("bad scout timezone, skipping", "tz", settings.TopicScout.Timezone)
		return
	}
	local := s.now().In(loc)
	hhmm := local.Format("15:04")
	hit := false
	for _, t := range settings.TopicScout.Times {
		if t == hhmm {
			hit = true
			break
		}
	}
	if !hit {
		return
	}

	planned := local.Truncate(time.Minute).UTC()
	newCount, err := s.q.CountNewRawItems(ctx)
	if err != nil {
		s.log.Error("count new raw_items failed", "error", err)
		return
	}
	note := ""
	skip := newCount < int64(settings.TopicScout.MinNewItems)
	if skip {
		note = fmt.Sprintf("skipped: new_items=%d < min_new_items=%d", newCount, settings.TopicScout.MinNewItems)
	}

	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		qtx := s.q.WithTx(tx)
		runID, err := qtx.RecordScheduleRun(ctx, dbgen.RecordScheduleRunParams{
			ScheduleKey: "topic_scout",
			PlannedAt:   pgtype.Timestamptz{Time: planned, Valid: true},
			Queue:       string(queue.TopicScout),
			Note:        pgtype.Text{String: note, Valid: note != ""},
		})
		if isNoRows(err) {
			return nil // 本时刻已处理过
		}
		if err != nil {
			return err
		}
		if skip {
			s.log.Info("topic_scout skipped", "note", note)
			return nil // 留痕但不入队
		}
		msgID, err := queue.Send(ctx, tx, queue.TopicScout, scheduledScoutPayload(),
			queue.WithTrigger("scheduler"))
		if err != nil {
			return err
		}
		s.log.Info("topic_scout enqueued", "msg_id", msgID, "new_items", newCount)
		return setScheduleRunMsg(ctx, tx, runID, msgID)
	})
	if err != nil {
		s.log.Error("enqueue topic_scout failed", "error", err)
	}
}

func scheduledScoutPayload() map[string]any {
	return map[string]any{"maxItems": scheduledScoutMaxItems}
}

func setScheduleRunMsg(ctx context.Context, tx pgx.Tx, runID uuid.UUID, msgID int64) error {
	_, err := tx.Exec(ctx, "update schedule_runs set msg_id = $1 where id = $2", msgID, runID)
	return err
}

func isNoRows(err error) bool {
	return err != nil && errors.Is(err, pgx.ErrNoRows)
}
