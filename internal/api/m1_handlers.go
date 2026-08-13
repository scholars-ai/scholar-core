package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/scholars-ai/scholar-core/internal/db/dbgen"
	"github.com/scholars-ai/scholar-core/internal/pipeline"
	"github.com/scholars-ai/scholar-core/internal/queue"
	"github.com/scholars-ai/scholar-core/internal/scheduler"
)

// ---- topics: approve / reject / evaluations ----

func (h *Server) ApproveTopic(w http.ResponseWriter, r *http.Request, topicId TopicId) {
	h.transition(w, r, topicId, dbgen.TopicStatusScored, dbgen.TopicStatusApproved)
}

func (h *Server) RejectTopic(w http.ResponseWriter, r *http.Request, topicId TopicId) {
	// candidate 和 scored 都允许人工否决（SPEC-002 §3）
	topic, err := h.q.GetTopic(r.Context(), topicId)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "not_found", "topic not found")
		return
	}
	if err != nil {
		h.internalError(w, "get topic", err)
		return
	}
	h.transition(w, r, topicId, topic.Status, dbgen.TopicStatusRejected)
}

// transition 执行一次状态机流转；非法流转返回 409（状态机是唯一裁判）。
func (h *Server) transition(w http.ResponseWriter, r *http.Request, id TopicId, from, to dbgen.TopicStatus) {
	if !pipeline.CanTopicTransition(from, to) {
		writeError(w, http.StatusConflict, "invalid_transition",
			fmt.Sprintf("cannot transition topic from %q to %q", from, to))
		return
	}
	topic, err := h.q.TransitionTopic(r.Context(), dbgen.TransitionTopicParams{
		ID: id, FromStatus: from, ToStatus: to,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		// CAS 失败：并发下状态已变（或 topic 不存在）
		if _, gerr := h.q.GetTopic(r.Context(), id); errors.Is(gerr, pgx.ErrNoRows) {
			writeError(w, http.StatusNotFound, "not_found", "topic not found")
			return
		}
		writeError(w, http.StatusConflict, "state_changed", "topic state changed concurrently; reload and retry")
		return
	}
	if err != nil {
		h.internalError(w, "transition topic", err)
		return
	}
	writeJSON(w, http.StatusOK, toAPITopic(&topic))
}

func (h *Server) ListTopicEvaluations(w http.ResponseWriter, r *http.Request, topicId TopicId) {
	if _, err := h.q.GetTopic(r.Context(), topicId); errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "not_found", "topic not found")
		return
	}
	evals, err := h.q.ListTopicEvaluations(r.Context(), topicId)
	if err != nil {
		h.internalError(w, "list evaluations", err)
		return
	}
	out := make([]TopicEvaluation, 0, len(evals))
	for i := range evals {
		out = append(out, toAPIEvaluation(&evals[i]))
	}
	writeJSON(w, http.StatusOK, out)
}

// ---- sources CRUD ----

func (h *Server) ListSources(w http.ResponseWriter, r *http.Request, params ListSourcesParams) {
	arg := pgtype.Bool{}
	if params.Enabled != nil {
		arg = pgtype.Bool{Bool: *params.Enabled, Valid: true}
	}
	rows, err := h.q.ListSources(r.Context(), arg)
	if err != nil {
		h.internalError(w, "list sources", err)
		return
	}
	out := make([]SourceWithHealth, 0, len(rows))
	for i := range rows {
		out = append(out, listRowToAPI(&rows[i]))
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *Server) GetSource(w http.ResponseWriter, r *http.Request, sourceId SourceId) {
	row, err := h.q.GetSource(r.Context(), sourceId)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "not_found", "source not found")
		return
	}
	if err != nil {
		h.internalError(w, "get source", err)
		return
	}
	out := getRowToAPI(&row)
	writeJSON(w, http.StatusOK, out)
}

func (h *Server) CreateSource(w http.ResponseWriter, r *http.Request) {
	var req SourceInput
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}
	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "name is required")
		return
	}
	if err := validateSourceURL(req.Type, req.Url); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_url", err.Error())
		return
	}
	weight := 0.5
	if req.Weight != nil {
		weight = float64(*req.Weight)
	}
	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}
	fcJSON, err := fetchConfigJSON(req.FetchConfig)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_fetch_config", err.Error())
		return
	}
	src, err := h.q.CreateSource(r.Context(), dbgen.CreateSourceParams{
		Name:        req.Name,
		Type:        dbgen.SourceType(req.Type),
		Url:         textOrNull(req.Url),
		Category:    dbgen.SourceCategory(req.Category),
		Weight:      floatNumeric(weight),
		Enabled:     enabled,
		FetchConfig: fcJSON,
	})
	if isUniqueViolation(err) {
		writeError(w, http.StatusConflict, "duplicate_name", "a source with this name already exists")
		return
	}
	if err != nil {
		h.internalError(w, "create source", err)
		return
	}
	writeJSON(w, http.StatusCreated, sourceToAPI(&src))
}

func (h *Server) UpdateSource(w http.ResponseWriter, r *http.Request, sourceId SourceId) {
	var req SourcePatch
	body, err := readAll(r)
	if err != nil || json.Unmarshal(body, &req) != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", "cannot parse body")
		return
	}
	// PATCH 语义：区分"没传 url"和"显式传 null 清空 url"
	var rawPatch map[string]json.RawMessage
	_ = json.Unmarshal(body, &rawPatch)
	_, setURL := rawPatch["url"]

	arg := dbgen.UpdateSourceParams{ID: sourceId, SetUrl: setURL}
	if req.Name != nil {
		arg.Name = pgtype.Text{String: *req.Name, Valid: true}
	}
	if setURL {
		arg.Url = textOrNull(req.Url)
	}
	if req.Category != nil {
		arg.Category = dbgen.NullSourceCategory{SourceCategory: dbgen.SourceCategory(*req.Category), Valid: true}
	}
	if req.Weight != nil {
		arg.Weight = floatNumeric(float64(*req.Weight))
	}
	if req.Enabled != nil {
		arg.Enabled = pgtype.Bool{Bool: *req.Enabled, Valid: true}
	}
	if req.FetchConfig != nil {
		fcJSON, ferr := fetchConfigJSON(req.FetchConfig)
		if ferr != nil {
			writeError(w, http.StatusBadRequest, "invalid_fetch_config", ferr.Error())
			return
		}
		arg.FetchConfig = fcJSON
	}
	src, err := h.q.UpdateSource(r.Context(), arg)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "not_found", "source not found")
		return
	}
	if err != nil {
		h.internalError(w, "update source", err)
		return
	}
	writeJSON(w, http.StatusOK, sourceToAPI(&src))
}

func (h *Server) DeleteSource(w http.ResponseWriter, r *http.Request, sourceId SourceId) {
	n, err := h.q.DeleteSource(r.Context(), sourceId)
	if err != nil {
		h.internalError(w, "delete source", err)
		return
	}
	if n == 0 {
		writeError(w, http.StatusNotFound, "not_found", "source not found")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// ---- 手动触发与投喂 ----

func (h *Server) TriggerSourceFetch(w http.ResponseWriter, r *http.Request, sourceId SourceId) {
	row, err := h.q.GetSource(r.Context(), sourceId)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "not_found", "source not found")
		return
	}
	if err != nil {
		h.internalError(w, "get source", err)
		return
	}
	if !row.Enabled {
		writeError(w, http.StatusConflict, "source_disabled", "enable the source before fetching")
		return
	}
	msgID, err := h.sendJob(r.Context(), queue.SourceFetch, map[string]string{"sourceId": sourceId.String()})
	if err != nil {
		h.internalError(w, "enqueue fetch", err)
		return
	}
	writeJSON(w, http.StatusAccepted, JobAccepted{Queue: string(queue.SourceFetch), MsgId: msgID})
}

func (h *Server) TriggerTopicScout(w http.ResponseWriter, r *http.Request) {
	msgID, err := h.sendJob(r.Context(), queue.TopicScout, map[string]any{"manual": true})
	if err != nil {
		h.internalError(w, "enqueue scout", err)
		return
	}
	writeJSON(w, http.StatusAccepted, JobAccepted{Queue: string(queue.TopicScout), MsgId: msgID})
}

func (h *Server) IngestUrl(w http.ResponseWriter, r *http.Request) {
	var req IngestUrlRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}
	if err := checkHTTPURL(req.Url); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_url", err.Error())
		return
	}
	// 确保 Manual Feed 源存在，然后把 URL 交给 agents 侧按 fetch_page 流程抓取
	srcID, err := h.q.GetManualSource(r.Context())
	if errors.Is(err, pgx.ErrNoRows) {
		srcID, err = h.q.CreateManualSource(r.Context())
	}
	if err != nil {
		h.internalError(w, "manual source", err)
		return
	}
	payload := map[string]string{"sourceId": srcID.String(), "url": req.Url}
	if req.Note != nil {
		payload["note"] = *req.Note
	}
	msgID, err := h.sendJob(r.Context(), queue.SourceFetch, payload)
	if err != nil {
		h.internalError(w, "enqueue ingest", err)
		return
	}
	writeJSON(w, http.StatusAccepted, JobAccepted{Queue: string(queue.SourceFetch), MsgId: msgID})
}

// ---- 调度设置 ----

func (h *Server) GetSchedulerSettings(w http.ResponseWriter, r *http.Request) {
	row, err := h.q.GetSchedulerSettings(r.Context())
	if errors.Is(err, pgx.ErrNoRows) {
		// 未 seed：返回默认值（首次启动 core 时 Seed 会写入）
		writeJSON(w, http.StatusOK, scheduler.DefaultSettings())
		return
	}
	if err != nil {
		h.internalError(w, "get settings", err)
		return
	}
	writeRaw(w, http.StatusOK, row.Settings)
}

func (h *Server) UpdateSchedulerSettings(w http.ResponseWriter, r *http.Request) {
	var patch SchedulerSettingsPatch
	if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}
	// 读当前值 → 合并 patch → 校验 → 整体写回
	current := scheduler.DefaultSettings()
	if row, err := h.q.GetSchedulerSettings(r.Context()); err == nil {
		_ = json.Unmarshal(row.Settings, &current)
	}
	if patch.SourceFetch != nil {
		current.SourceFetch.Enabled = patch.SourceFetch.Enabled
		current.SourceFetch.DefaultIntervalMinutes = patch.SourceFetch.DefaultIntervalMinutes
	}
	if patch.TopicScout != nil {
		current.TopicScout.Enabled = patch.TopicScout.Enabled
		current.TopicScout.Times = patch.TopicScout.Times
		current.TopicScout.Timezone = patch.TopicScout.Timezone
		current.TopicScout.MinNewItems = patch.TopicScout.MinNewItems
	}
	if patch.TopicEvaluate != nil {
		current.TopicEvaluate.Enabled = patch.TopicEvaluate.Enabled
		current.TopicEvaluate.MaxConcurrency = patch.TopicEvaluate.MaxConcurrency
	}
	if err := validateSettings(current); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_settings", err.Error())
		return
	}
	raw, err := json.Marshal(current)
	if err != nil {
		h.internalError(w, "marshal settings", err)
		return
	}
	row, err := h.q.UpsertSchedulerSettings(r.Context(), raw)
	if err != nil {
		h.internalError(w, "save settings", err)
		return
	}
	writeRaw(w, http.StatusOK, row.Settings)
}

// validateSettings：API 层的最后防线，非法配置绝不入库（scheduler 直接信任 DB 内容）。
func validateSettings(s scheduler.Settings) error {
	if s.SourceFetch.DefaultIntervalMinutes < 5 || s.SourceFetch.DefaultIntervalMinutes > 10080 {
		return fmt.Errorf("sourceFetch.defaultIntervalMinutes must be 5..10080, got %d", s.SourceFetch.DefaultIntervalMinutes)
	}
	if len(s.TopicScout.Times) == 0 || len(s.TopicScout.Times) > 24 {
		return errors.New("topicScout.times must have 1..24 entries")
	}
	seen := map[string]bool{}
	for _, t := range s.TopicScout.Times {
		var hh, mm int
		if _, err := fmt.Sscanf(t, "%02d:%02d", &hh, &mm); err != nil || hh > 23 || mm > 59 || len(t) != 5 {
			return fmt.Errorf("bad time %q (want HH:MM)", t)
		}
		if seen[t] {
			return fmt.Errorf("duplicate time %q", t)
		}
		seen[t] = true
	}
	if _, err := time.LoadLocation(s.TopicScout.Timezone); err != nil {
		return fmt.Errorf("unknown timezone %q", s.TopicScout.Timezone)
	}
	if s.TopicScout.MinNewItems < 0 || s.TopicScout.MinNewItems > 1000 {
		return errors.New("topicScout.minNewItems must be 0..1000")
	}
	if s.TopicEvaluate.MaxConcurrency < 1 || s.TopicEvaluate.MaxConcurrency > 32 {
		return errors.New("topicEvaluate.maxConcurrency must be 1..32")
	}
	return nil
}

// ---- helpers ----

func (h *Server) sendJob(ctx context.Context, q queue.Name, payload any) (int64, error) {
	var msgID int64
	err := pgx.BeginFunc(ctx, h.pool, func(tx pgx.Tx) error {
		var err error
		msgID, err = queue.Send(ctx, tx, q, payload)
		return err
	})
	return msgID, err
}

func validateSourceURL(t SourceType, u *string) error {
	if t == Manual {
		return nil // manual 源无需 URL
	}
	if u == nil || *u == "" {
		return fmt.Errorf("url is required for type %q", t)
	}
	return checkHTTPURL(*u)
}

func checkHTTPURL(raw string) error {
	p, err := url.Parse(raw)
	if err != nil || (p.Scheme != "http" && p.Scheme != "https") || p.Host == "" {
		return fmt.Errorf("not a valid http(s) URL: %q", raw)
	}
	return nil
}

func fetchConfigJSON(fc *SourceFetchConfig) ([]byte, error) {
	if fc == nil {
		return []byte(`{}`), nil
	}
	return json.Marshal(fc)
}

func isUniqueViolation(err error) bool {
	return err != nil && strings.Contains(err.Error(), "23505")
}

func textOrNull(s *string) pgtype.Text {
	if s == nil {
		return pgtype.Text{}
	}
	return pgtype.Text{String: *s, Valid: true}
}

func floatNumeric(f float64) pgtype.Numeric {
	var n pgtype.Numeric
	_ = n.Scan(fmt.Sprintf("%.4f", f))
	return n
}

func writeRaw(w http.ResponseWriter, status int, raw []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(raw)
}

func readAll(r *http.Request) ([]byte, error) {
	defer func() { _ = r.Body.Close() }()
	return io.ReadAll(http.MaxBytesReader(nil, r.Body, 1<<20))
}

// ---- API 类型转换 ----

func toAPIEvaluation(e *dbgen.ListTopicEvaluationsRow) TopicEvaluation {
	var scores map[string]float32
	_ = json.Unmarshal(e.DimensionScores, &scores)
	total, _ := e.TotalScore.Float64Value()
	out := TopicEvaluation{
		Id:              e.ID,
		TopicId:         e.TopicID,
		RubricVersion:   e.RubricVersion,
		DimensionScores: scores,
		TotalScore:      float32(total.Float64),
		Rationale:       e.Rationale,
		JudgeModel:      e.JudgeModel,
		CreatedAt:       e.CreatedAt.Time,
	}
	if e.AgentRunID.Valid {
		id := e.AgentRunID.UUID
		out.AgentRunId = &id
	}
	if e.WeightVersion.Valid {
		version := int(e.WeightVersion.Int32)
		out.WeightVersion = &version
	}
	if e.VetoedDimension.Valid {
		vetoed := e.VetoedDimension.String
		out.VetoedDimension = &vetoed
	}
	return out
}
