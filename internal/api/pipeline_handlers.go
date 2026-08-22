package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/scholars-ai/scholar-core/internal/scheduler"
)

type pipelineCounts struct {
	RawTotal, RawNew, RawClustered, RawDiscarded        int64
	TopicTotal, TopicScored, TopicPassed, TopicRejected int64
	ArticleTotal, ArticleReady, ArticlePassed           int64
	ArticleRejected, ArticleRewrites                    int64
}

func (h *Server) GetPipelineSummary(w http.ResponseWriter, r *http.Request) {
	countsRow, err := h.q.GetPipelineCounts(r.Context())
	if err != nil {
		h.internalError(w, "get pipeline counts", err)
		return
	}
	runs, err := h.q.LastPipelineScheduleRuns(r.Context())
	if err != nil {
		h.internalError(w, "get pipeline schedule runs", err)
		return
	}
	failures, err := h.q.RecentPipelineFailures(r.Context(), 10)
	if err != nil {
		h.internalError(w, "get pipeline failures", err)
		return
	}

	settings := scheduler.DefaultSettings()
	if row, settingsErr := h.q.GetSchedulerSettings(r.Context()); settingsErr == nil {
		_ = json.Unmarshal(row.Settings, &settings)
	} else if settingsErr != pgx.ErrNoRows {
		h.internalError(w, "get scheduler settings", settingsErr)
		return
	}

	lastRuns := make(map[string]time.Time, len(runs))
	for _, run := range runs {
		if run.PlannedAt.Valid {
			lastRuns[run.StageKey] = run.PlannedAt.Time
		}
	}
	recentErrors := make([]PipelineError, 0, len(failures))
	for _, failure := range failures {
		recentErrors = append(recentErrors, PipelineError{
			Id: failure.ID, Queue: failure.Queue, ErrorType: failure.ErrorType,
			Message: failure.ErrorMessage, Retryable: failure.Retryable,
			CreatedAt: failure.CreatedAt.Time,
		})
	}

	now := time.Now().UTC()
	writeJSON(w, http.StatusOK, PipelineSummary{
		GeneratedAt: now,
		Stages: buildPipelineStages(pipelineCounts{
			RawTotal: countsRow.RawTotal, RawNew: countsRow.RawNew,
			RawClustered: countsRow.RawClustered, RawDiscarded: countsRow.RawDiscarded,
			TopicTotal: countsRow.TopicTotal, TopicScored: countsRow.TopicScored,
			TopicPassed: countsRow.TopicPassed, TopicRejected: countsRow.TopicRejected,
			ArticleTotal: countsRow.ArticleTotal, ArticleReady: countsRow.ArticleReady,
			ArticlePassed: countsRow.ArticlePassed, ArticleRejected: countsRow.ArticleRejected,
			ArticleRewrites: countsRow.ArticleRewrites,
		}, settings, lastRuns, now),
		RecentErrors: recentErrors,
	})
}

func (h *Server) ListPipelineRuns(w http.ResponseWriter, r *http.Request, params ListPipelineRunsParams) {
	limit := 50
	if params.Limit != nil {
		limit = *params.Limit
	}
	runs, err := h.q.RecentScheduleRuns(r.Context(), int32(limit))
	if err != nil {
		h.internalError(w, "list pipeline runs", err)
		return
	}
	items := make([]PipelineRun, 0, len(runs))
	for _, run := range runs {
		var msgID *int64
		if run.MsgID.Valid {
			value := run.MsgID.Int64
			msgID = &value
		}
		var note *string
		if run.Note.Valid {
			value := run.Note.String
			note = &value
		}
		items = append(items, PipelineRun{
			Id: run.ID, ScheduleKey: run.ScheduleKey,
			PlannedAt: run.PlannedAt.Time, EnqueuedAt: run.EnqueuedAt.Time,
			Queue: run.Queue, MsgId: msgID, Note: note,
		})
	}
	writeJSON(w, http.StatusOK, PipelineRunList{Items: items})
}

func buildPipelineStages(counts pipelineCounts, settings scheduler.Settings, lastRuns map[string]time.Time, now time.Time) []PipelineStageSummary {
	last := func(key string) *time.Time {
		value, ok := lastRuns[key]
		if !ok {
			return nil
		}
		copy := value
		return &copy
	}
	sourceLast := last("source_fetch")
	var sourceNext *time.Time
	if settings.SourceFetch.Enabled {
		value := nextIntervalRun(sourceLast, time.Duration(settings.SourceFetch.DefaultIntervalMinutes)*time.Minute, now)
		sourceNext = &value
	}
	return []PipelineStageSummary{
		{Key: PipelineStageSummaryKeySourceFetch, Label: "资讯采集", CadenceMinutes: settings.SourceFetch.DefaultIntervalMinutes,
			Total: int(counts.RawTotal), Ready: int(counts.RawNew), Passed: int(counts.RawClustered),
			Failed: int(counts.RawDiscarded), Rewrites: 0, LastRunAt: sourceLast, NextRunAt: sourceNext},
		{Key: PipelineStageSummaryKeyTopicScout, Label: "Topic 生成与评分", CadenceMinutes: 240,
			Total: int(counts.TopicTotal), Ready: int(counts.TopicScored), Passed: int(counts.TopicPassed),
			Failed: int(counts.TopicRejected), Rewrites: 0, LastRunAt: last("topic_scout"),
			NextRunAt: nextDailyRun(settings.TopicScout.Times, settings.TopicScout.Timezone, now)},
		{Key: PipelineStageSummaryKeyArticleWrite, Label: "平台文章生成", CadenceMinutes: 480,
			Total: int(counts.ArticleTotal), Ready: int(counts.ArticleReady), Passed: int(counts.ArticlePassed),
			Failed: int(counts.ArticleRejected), Rewrites: int(counts.ArticleRewrites), LastRunAt: last("article_write"),
			NextRunAt: nextDailyRun(settings.ArticleWrite.Times, settings.ArticleWrite.Timezone, now)},
	}
}

func nextDailyRun(times []string, timezone string, now time.Time) *time.Time {
	location, err := time.LoadLocation(timezone)
	if err != nil || len(times) == 0 {
		return nil
	}
	localNow := now.In(location)
	for dayOffset := 0; dayOffset <= 1; dayOffset++ {
		day := localNow.AddDate(0, 0, dayOffset)
		var earliest *time.Time
		for _, value := range times {
			parsed, parseErr := time.Parse("15:04", value)
			if parseErr != nil {
				continue
			}
			candidate := time.Date(day.Year(), day.Month(), day.Day(), parsed.Hour(), parsed.Minute(), 0, 0, location)
			if candidate.Before(localNow) || candidate.Equal(localNow) {
				continue
			}
			if earliest == nil || candidate.Before(*earliest) {
				copy := candidate
				earliest = &copy
			}
		}
		if earliest != nil {
			return earliest
		}
	}
	return nil
}

func nextIntervalRun(last *time.Time, interval time.Duration, now time.Time) time.Time {
	if last == nil {
		return now.Add(interval)
	}
	next := last.Add(interval)
	for !next.After(now) {
		next = next.Add(interval)
	}
	return next
}
