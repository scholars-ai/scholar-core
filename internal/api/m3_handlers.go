package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/scholars-ai/scholar-core/internal/db/dbgen"
	"github.com/scholars-ai/scholar-core/internal/queue"
)

var validMetricWindows = map[MetricWindow]bool{H24: true, H72: true, D7: true, Custom: true}

func (h *Server) ListPublications(w http.ResponseWriter, r *http.Request, params ListPublicationsParams) {
	platform := dbgen.NullPlatform{}
	if params.Platform != nil {
		if !validPlatforms[*params.Platform] {
			writeError(w, http.StatusBadRequest, "invalid_platform", fmt.Sprintf("unknown platform %q", *params.Platform))
			return
		}
		platform = dbgen.NullPlatform{Platform: dbgen.Platform(*params.Platform), Valid: true}
	}
	// remindersOnly 必须先计算每条 Publication 的标准窗口完成度再分页。M3 单用户阶段
	// 读取上限 200 足够，并避免把复杂提醒规则复制到 SQL。
	rows, err := h.q.ListPublicationsForMetrics(r.Context(), dbgen.ListPublicationsForMetricsParams{
		Platform: platform, Lim: 200,
	})
	if err != nil {
		h.internalError(w, "list publications for metrics", err)
		return
	}
	now := time.Now().UTC()
	items := make([]PublicationPerformance, 0, len(rows))
	overdueCount := 0
	for i := range rows {
		snapshots, err := h.q.ListMetricSnapshots(r.Context(), rows[i].ID)
		if err != nil {
			h.internalError(w, "list publication snapshots", err)
			return
		}
		apiSnapshots := metricSnapshotsToAPI(snapshots)
		reminders := dueMetricReminders(rows[i].PublishedAt.Time, apiSnapshots, now)
		overdueCount += len(reminders)
		if params.RemindersOnly != nil && *params.RemindersOnly && len(reminders) == 0 {
			continue
		}
		items = append(items, PublicationPerformance{
			Publication: publicationMetricsRowToAPI(&rows[i]), ArticleTitle: rows[i].ArticleTitle,
			TopicTitle: rows[i].TopicTitle, Snapshots: apiSnapshots, Reminders: reminders,
		})
	}
	total := len(items)
	offset, limit := 0, 50
	if params.Offset != nil {
		offset = *params.Offset
	}
	if params.Limit != nil {
		limit = *params.Limit
	}
	if offset > len(items) {
		offset = len(items)
	}
	end := min(offset+limit, len(items))
	writeJSON(w, http.StatusOK, PublicationPerformanceList{
		Items: items[offset:end], Total: total, OverdueCount: overdueCount,
	})
}

func (h *Server) ListMetricSnapshots(w http.ResponseWriter, r *http.Request, publicationId PublicationId) {
	if _, err := h.q.GetPublicationForMetrics(r.Context(), publicationId); errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "not_found", "publication not found")
		return
	} else if err != nil {
		h.internalError(w, "get publication", err)
		return
	}
	rows, err := h.q.ListMetricSnapshots(r.Context(), publicationId)
	if err != nil {
		h.internalError(w, "list metric snapshots", err)
		return
	}
	writeJSON(w, http.StatusOK, metricSnapshotsToAPI(rows))
}

func (h *Server) CreateMetricSnapshot(w http.ResponseWriter, r *http.Request, publicationId PublicationId) {
	var req CreateMetricSnapshotRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}
	created, err := h.insertMetricSnapshots(r, []metricInsert{{
		publicationID: publicationId, window: req.SnapshotWindow,
		capturedAt: req.CapturedAt, metrics: req.Metrics, source: dbgen.MetricSourceManual,
	}})
	if h.writeMetricInsertError(w, err) {
		return
	}
	writeJSON(w, http.StatusCreated, created[0])
}

func (h *Server) ImportMetricSnapshots(w http.ResponseWriter, r *http.Request) {
	var req ImportMetricSnapshotsRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4<<20)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}
	if len(req.Items) == 0 || len(req.Items) > 500 {
		writeError(w, http.StatusBadRequest, "invalid_request", "items must contain 1..500 rows")
		return
	}
	items := make([]metricInsert, 0, len(req.Items))
	for _, item := range req.Items {
		items = append(items, metricInsert{
			publicationID: item.PublicationId, window: item.SnapshotWindow,
			capturedAt: item.CapturedAt, metrics: item.Metrics, source: dbgen.MetricSourceImport,
		})
	}
	created, err := h.insertMetricSnapshots(r, items)
	if h.writeMetricInsertError(w, err) {
		return
	}
	writeJSON(w, http.StatusCreated, ImportMetricSnapshotsResult{Imported: len(created), Snapshots: created})
}

type metricInsert struct {
	publicationID uuid.UUID
	window        MetricWindow
	capturedAt    time.Time
	metrics       EngagementMetrics
	source        dbgen.MetricSource
}

type metricInsertError struct {
	status int
	code   string
	msg    string
}

func (e *metricInsertError) Error() string { return e.msg }

func (h *Server) insertMetricSnapshots(r *http.Request, items []metricInsert) ([]MetricSnapshot, error) {
	createdIDs := make([]uuid.UUID, 0, len(items))
	affected := map[string]struct{}{}
	err := pgx.BeginFunc(r.Context(), h.pool, func(tx pgx.Tx) error {
		qtx := h.q.WithTx(tx)
		for index, item := range items {
			if err := validateMetricInput(item.window, item.capturedAt, item.metrics); err != nil {
				return &metricInsertError{http.StatusBadRequest, "invalid_metric", fmt.Sprintf("row %d: %v", index+1, err)}
			}
			publication, err := qtx.GetPublicationForMetrics(r.Context(), item.publicationID)
			if errors.Is(err, pgx.ErrNoRows) {
				return &metricInsertError{http.StatusNotFound, "not_found", fmt.Sprintf("row %d: publication not found", index+1)}
			}
			if err != nil {
				return err
			}
			if item.capturedAt.Before(publication.PublishedAt.Time) {
				return &metricInsertError{http.StatusBadRequest, "invalid_metric", fmt.Sprintf("row %d: capturedAt cannot precede publishedAt", index+1)}
			}
			weights, err := qtx.ActivePerformanceWeights(r.Context(), publication.Platform)
			if err != nil {
				return err
			}
			raw, err := performanceRaw(item.metrics, weights.Weights)
			if err != nil {
				return err
			}
			metricsJSON, err := json.Marshal(item.metrics)
			if err != nil {
				return err
			}
			row, err := qtx.CreateMetricSnapshot(r.Context(), dbgen.CreateMetricSnapshotParams{
				PublicationID: item.publicationID,
				CapturedAt:    pgtype.Timestamptz{Time: item.capturedAt.UTC(), Valid: true},
				Metrics:       metricsJSON, Source: item.source, SnapshotWindow: dbgen.MetricWindow(item.window),
				PerformanceRaw: numeric(raw), PerformanceWeightVersion: weights.Version,
			})
			if isUniqueViolation(err) {
				return &metricInsertError{http.StatusConflict, "duplicate_snapshot", fmt.Sprintf("row %d: this publication/window or capturedAt already exists", index+1)}
			}
			if err != nil {
				return err
			}
			createdIDs = append(createdIDs, row.ID)
			if item.window != Custom {
				affected[string(publication.Platform)+":"+string(item.window)] = struct{}{}
			}
		}
		for key := range affected {
			parts := strings.SplitN(key, ":", 2)
			if err := qtx.RecomputePerformancePercentiles(r.Context(), dbgen.RecomputePerformancePercentilesParams{
				Platform: dbgen.Platform(parts[0]), SnapshotWindow: dbgen.MetricWindow(parts[1]),
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	createdSet := make(map[uuid.UUID]struct{}, len(createdIDs))
	for _, id := range createdIDs {
		createdSet[id] = struct{}{}
	}
	out := make([]MetricSnapshot, 0, len(createdIDs))
	for _, item := range items {
		rows, err := h.q.ListMetricSnapshots(r.Context(), item.publicationID)
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			if _, ok := createdSet[row.ID]; ok {
				out = append(out, metricSnapshotToAPI(&row))
				delete(createdSet, row.ID)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CapturedAt.Before(out[j].CapturedAt) })
	return out, nil
}

func (h *Server) writeMetricInsertError(w http.ResponseWriter, err error) bool {
	if err == nil {
		return false
	}
	var inputErr *metricInsertError
	if errors.As(err, &inputErr) {
		writeError(w, inputErr.status, inputErr.code, inputErr.msg)
		return true
	}
	h.internalError(w, "insert metric snapshots", err)
	return true
}

func validateMetricInput(window MetricWindow, captured time.Time, metrics EngagementMetrics) error {
	if !validMetricWindows[window] {
		return fmt.Errorf("unknown snapshotWindow %q", window)
	}
	if captured.IsZero() || captured.After(time.Now().UTC().Add(5*time.Minute)) {
		return errors.New("capturedAt is required and cannot be in the future")
	}
	values := []*int{metrics.Views, metrics.Likes, metrics.Favorites, metrics.Comments, metrics.Shares, metrics.Follows}
	hasValue := false
	for _, value := range values {
		if value == nil {
			continue
		}
		hasValue = true
		if *value < 0 {
			return errors.New("metrics must be non-negative")
		}
	}
	if !hasValue {
		return errors.New("at least one metric is required")
	}
	return nil
}

func performanceRaw(metrics EngagementMetrics, rawWeights []byte) (float64, error) {
	weights := map[string]float64{}
	if err := json.Unmarshal(rawWeights, &weights); err != nil {
		return 0, fmt.Errorf("decode performance weights: %w", err)
	}
	values := map[string]*int{
		"views": metrics.Views, "likes": metrics.Likes, "favorites": metrics.Favorites,
		"comments": metrics.Comments, "shares": metrics.Shares, "follows": metrics.Follows,
	}
	var score float64
	for key, weight := range weights {
		if value := values[key]; value != nil {
			score += float64(*value) * weight
		}
	}
	return score, nil
}

func numeric(value float64) pgtype.Numeric {
	var out pgtype.Numeric
	_ = out.Scan(fmt.Sprintf("%.6f", value))
	return out
}

func (h *Server) GetPerformanceDashboard(w http.ResponseWriter, r *http.Request, params GetPerformanceDashboardParams) {
	days := 90
	if params.Days != nil {
		days = *params.Days
	}
	if days < 7 || days > 365 {
		writeError(w, http.StatusBadRequest, "invalid_days", "days must be 7..365")
		return
	}
	var platform any
	if params.Platform != nil {
		if !validPlatforms[*params.Platform] {
			writeError(w, http.StatusBadRequest, "invalid_platform", fmt.Sprintf("unknown platform %q", *params.Platform))
			return
		}
		platform = string(*params.Platform)
	}
	cutoff := time.Now().UTC().AddDate(0, 0, -days)
	rows, err := h.pool.Query(r.Context(), `
select p.platform::text, count(distinct p.id), count(ms.id),
       count(ms.id) filter (where ms.snapshot_window='h24'),
       count(ms.id) filter (where ms.snapshot_window='h72'),
       count(ms.id) filter (where ms.snapshot_window='d7'),
       count(ms.id) filter (where ms.performance_percentile >= 75),
       count(ms.id) filter (where ms.performance_percentile <= 25)
from publications p left join metric_snapshots ms on ms.publication_id=p.id and ms.captured_at >= $1
where p.published_at >= $1 and ($2::text is null or p.platform::text=$2)
group by p.platform order by p.platform`, cutoff, platform)
	if err != nil {
		h.internalError(w, "performance summaries", err)
		return
	}
	summaries := []PlatformPerformanceSummary{}
	for rows.Next() {
		var item PlatformPerformanceSummary
		if err := rows.Scan(&item.Platform, &item.Publications, &item.Snapshots, &item.Complete24h,
			&item.Complete72h, &item.Complete7d, &item.HighCount, &item.LowCount); err != nil {
			rows.Close()
			h.internalError(w, "scan performance summary", err)
			return
		}
		summaries = append(summaries, item)
	}
	rows.Close()

	caseRows, err := h.pool.Query(r.Context(), `
select p.id, p.article_id, p.platform::text, a.title, ms.snapshot_window::text,
       ms.performance_percentile, ms.performance_raw, ms.captured_at
from metric_snapshots ms join publications p on p.id=ms.publication_id
join articles a on a.id=p.article_id
where ms.captured_at >= $1 and ms.performance_percentile is not null
  and (ms.performance_percentile >= 75 or ms.performance_percentile <= 25)
  and ($2::text is null or p.platform::text=$2)
order by ms.captured_at desc limit 40`, cutoff, platform)
	if err != nil {
		h.internalError(w, "performance cases", err)
		return
	}
	cases := []PerformanceCase{}
	for caseRows.Next() {
		var item PerformanceCase
		var percentile, raw pgtype.Numeric
		if err := caseRows.Scan(&item.PublicationId, &item.ArticleId, &item.Platform, &item.Title,
			&item.SnapshotWindow, &percentile, &raw, &item.CapturedAt); err != nil {
			caseRows.Close()
			h.internalError(w, "scan performance case", err)
			return
		}
		item.Percentile = numericFloat32(percentile)
		item.PerformanceRaw = numericFloat32(raw)
		item.Band = High
		if item.Percentile <= 25 {
			item.Band = Low
		}
		cases = append(cases, item)
	}
	caseRows.Close()
	writeJSON(w, http.StatusOK, PerformanceDashboard{Days: days, Summaries: summaries, Cases: cases})
}

func (h *Server) ListInsights(w http.ResponseWriter, r *http.Request, params ListInsightsParams) {
	arg := dbgen.ListInsightsParams{Lim: 100}
	if params.Limit != nil {
		arg.Lim = int32(*params.Limit)
	}
	if params.Kind != nil {
		if *params.Kind != TopicLesson && *params.Kind != WritingLesson && *params.Kind != PlatformLesson && *params.Kind != SourceLesson {
			writeError(w, http.StatusBadRequest, "invalid_kind", "unknown insight kind")
			return
		}
		arg.Kind = dbgen.NullInsightKind{InsightKind: dbgen.InsightKind(*params.Kind), Valid: true}
	}
	if params.Status != nil {
		if *params.Status != InsightStatusCandidate && *params.Status != InsightStatusActive && *params.Status != InsightStatusRetired {
			writeError(w, http.StatusBadRequest, "invalid_status", "unknown insight status")
			return
		}
		arg.Status = dbgen.NullInsightStatus{InsightStatus: dbgen.InsightStatus(*params.Status), Valid: true}
	}
	if params.Platform != nil {
		if !validPlatforms[*params.Platform] {
			writeError(w, http.StatusBadRequest, "invalid_platform", "unknown platform")
			return
		}
		arg.Platform = dbgen.NullPlatform{Platform: dbgen.Platform(*params.Platform), Valid: true}
	}
	rows, err := h.q.ListInsights(r.Context(), arg)
	if err != nil {
		h.internalError(w, "list insights", err)
		return
	}
	out := make([]Insight, 0, len(rows))
	for i := range rows {
		out = append(out, insightToAPI(&rows[i]))
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *Server) UpdateInsight(w http.ResponseWriter, r *http.Request, insightId InsightId) {
	var req UpdateInsightRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}
	if req.Status != UpdateInsightRequestStatus(InsightStatusActive) && req.Status != UpdateInsightRequestStatus(InsightStatusRetired) {
		writeError(w, http.StatusBadRequest, "invalid_status", "status must be active or retired")
		return
	}
	row, err := h.q.UpdateInsightStatus(r.Context(), dbgen.UpdateInsightStatusParams{
		ID: insightId, Status: dbgen.InsightStatus(req.Status),
	})
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "not_found", "insight not found")
		return
	}
	if err != nil {
		h.internalError(w, "update insight", err)
		return
	}
	writeJSON(w, http.StatusOK, insightToAPI(&row))
}

func (h *Server) ListWeeklyReports(w http.ResponseWriter, r *http.Request, params ListWeeklyReportsParams) {
	limit := int32(12)
	if params.Limit != nil {
		limit = int32(*params.Limit)
	}
	rows, err := h.q.ListWeeklyReports(r.Context(), limit)
	if err != nil {
		h.internalError(w, "list weekly reports", err)
		return
	}
	out := make([]WeeklyReport, 0, len(rows))
	for i := range rows {
		out = append(out, weeklyReportToAPI(&rows[i]))
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *Server) TriggerMemoryReflect(w http.ResponseWriter, r *http.Request) {
	now := time.Now().UTC()
	start, end := now.AddDate(0, 0, -7), now
	if r.Body != nil {
		var req TriggerMemoryReflectRequest
		err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req)
		if err != nil && !errors.Is(err, io.EOF) {
			writeError(w, http.StatusBadRequest, "invalid_json", err.Error())
			return
		}
		if req.PeriodStart != nil {
			start = req.PeriodStart.UTC()
		}
		if req.PeriodEnd != nil {
			end = req.PeriodEnd.UTC()
		}
	}
	if !end.After(start) || end.Sub(start) > 90*24*time.Hour {
		writeError(w, http.StatusBadRequest, "invalid_period", "periodEnd must be after periodStart and span at most 90 days")
		return
	}
	msgID, err := h.sendJob(r.Context(), queue.MemoryReflect, map[string]string{
		"periodStart": start.Format(time.RFC3339), "periodEnd": end.Format(time.RFC3339),
	})
	if err != nil {
		h.internalError(w, "enqueue memory reflect", err)
		return
	}
	writeJSON(w, http.StatusAccepted, JobAccepted{Queue: string(queue.MemoryReflect), MsgId: msgID})
}

func dueMetricReminders(published time.Time, snapshots []MetricSnapshot, now time.Time) []MetricReminder {
	completed := map[MetricWindow]bool{}
	for _, snapshot := range snapshots {
		completed[snapshot.SnapshotWindow] = true
	}
	windows := []struct {
		name  MetricWindow
		delay time.Duration
	}{{H24, 24 * time.Hour}, {H72, 72 * time.Hour}, {D7, 7 * 24 * time.Hour}}
	out := []MetricReminder{}
	for _, window := range windows {
		due := published.Add(window.delay)
		if completed[window.name] || now.Before(due) {
			continue
		}
		out = append(out, MetricReminder{SnapshotWindow: window.name, DueAt: due, Overdue: true})
	}
	return out
}

func metricSnapshotsToAPI(rows []dbgen.MetricSnapshot) []MetricSnapshot {
	out := make([]MetricSnapshot, 0, len(rows))
	for i := range rows {
		out = append(out, metricSnapshotToAPI(&rows[i]))
	}
	return out
}

func metricSnapshotToAPI(row *dbgen.MetricSnapshot) MetricSnapshot {
	metrics := EngagementMetrics{}
	_ = json.Unmarshal(row.Metrics, &metrics)
	out := MetricSnapshot{
		Id: row.ID, PublicationId: row.PublicationID, CapturedAt: row.CapturedAt.Time,
		CreatedAt: row.CreatedAt.Time, Metrics: metrics, Source: MetricSource(row.Source),
		SnapshotWindow: MetricWindow(row.SnapshotWindow), PerformanceRaw: numericFloat32(row.PerformanceRaw),
		PerformanceWeightVersion: int(row.PerformanceWeightVersion),
	}
	if row.PerformancePercentile.Valid {
		value := numericFloat32(row.PerformancePercentile)
		out.PerformancePercentile = &value
	}
	return out
}

func publicationMetricsRowToAPI(row *dbgen.ListPublicationsForMetricsRow) Publication {
	publication := dbgen.Publication{
		ID: row.ID, ArticleID: row.ArticleID, Platform: row.Platform, PlatformPostID: row.PlatformPostID,
		PublishedAt: row.PublishedAt, FinalContentDiff: row.FinalContentDiff,
		FollowerCountAtPublish: row.FollowerCountAtPublish, CreatedAt: row.CreatedAt, EditRatio: row.EditRatio,
	}
	return toAPIPublication(&publication)
}

func insightToAPI(row *dbgen.Insight) Insight {
	evidence := []InsightEvidence{}
	_ = json.Unmarshal(row.Evidence, &evidence)
	out := Insight{
		Id: row.ID, Kind: InsightKind(row.Kind), Content: row.Content, Evidence: evidence,
		Confidence: numericFloat32(row.Confidence), Status: InsightStatus(row.Status),
		CreatedAt: row.CreatedAt.Time, UpdatedAt: row.UpdatedAt.Time,
	}
	if row.Platform.Valid {
		platform := Platform(row.Platform.Platform)
		out.Platform = &platform
	}
	return out
}

func weeklyReportToAPI(row *dbgen.WeeklyReport) WeeklyReport {
	calibration := CalibrationReport{Correlations: []CorrelationResult{}, HighCases: []PerformanceCase{}, LowCases: []PerformanceCase{}}
	_ = json.Unmarshal(row.Calibration, &calibration)
	out := WeeklyReport{
		Id: row.ID, PeriodStart: row.PeriodStart.Time, PeriodEnd: row.PeriodEnd.Time,
		SampleCount: int(row.SampleCount), SummaryMarkdown: row.SummaryMarkdown,
		Calibration: calibration, CreatedAt: row.CreatedAt.Time,
	}
	if row.AgentRunID.Valid {
		value := row.AgentRunID.UUID
		out.AgentRunId = &value
	}
	return out
}

func numericFloat32(value pgtype.Numeric) float32 {
	converted, err := value.Float64Value()
	if err != nil || !converted.Valid || math.IsNaN(converted.Float64) {
		return 0
	}
	return float32(converted.Float64)
}
