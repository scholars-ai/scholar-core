package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/scholars-ai/scholar-core/internal/db/dbgen"
)

// Server 实现 oapi-codegen 生成的 ServerInterface。
type Server struct {
	pool *pgxpool.Pool
	q    *dbgen.Queries
	log  *slog.Logger
}

func NewServer(pool *pgxpool.Pool, log *slog.Logger) *Server {
	return &Server{pool: pool, q: dbgen.New(pool), log: log}
}

var _ ServerInterface = (*Server)(nil)

func (h *Server) GetHealth(w http.ResponseWriter, r *http.Request) {
	db := HealthDb("ok")
	if err := h.pool.Ping(r.Context()); err != nil {
		h.log.Error("health: db ping failed", "error", err)
		db = "down"
	}
	writeJSON(w, http.StatusOK, Health{Status: "ok", Db: db})
}

// validTopicStatuses 与 SPEC-002 §3 的状态机、DB enum、shared 契约三处对齐。
// oapi-codegen 只生成类型别名不做运行时校验，非法值若直传 DB 会引发 enum 转换错误
// 并冒成 500 —— 那是客户端错误，必须在这里拦成 400。
var validTopicStatuses = map[TopicStatus]bool{
	TopicStatusCandidate: true, TopicStatusScored: true, TopicStatusApproved: true,
	TopicStatusInWriting: true, TopicStatusWritten: true, TopicStatusRejected: true,
}

func (h *Server) ListTopics(w http.ResponseWriter, r *http.Request, params ListTopicsParams) {
	arg := dbgen.ListTopicsParams{Lim: 50}
	if params.Status != nil {
		if !validTopicStatuses[*params.Status] {
			writeError(w, http.StatusBadRequest, "invalid_status",
				fmt.Sprintf("unknown topic status %q", string(*params.Status)))
			return
		}
		arg.Status = dbgen.NullTopicStatus{TopicStatus: dbgen.TopicStatus(*params.Status), Valid: true}
	}
	if params.Limit != nil {
		arg.Lim = int32(*params.Limit)
	}
	if params.Offset != nil {
		arg.Off = int32(*params.Offset)
	}

	topics, err := h.q.ListTopics(r.Context(), arg)
	if err != nil {
		h.internalError(w, "list topics", err)
		return
	}
	total, err := h.q.CountTopics(r.Context(), arg.Status)
	if err != nil {
		h.internalError(w, "count topics", err)
		return
	}

	items := make([]Topic, 0, len(topics))
	for i := range topics {
		items = append(items, toAPITopic(&topics[i]))
	}
	writeJSON(w, http.StatusOK, struct {
		Items []Topic `json:"items"`
		Total int64   `json:"total"`
	}{Items: items, Total: total})
}

func (h *Server) CreateManualTopic(w http.ResponseWriter, r *http.Request) {
	var req CreateTopicRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}
	if req.Title == "" || len(req.TargetPlatforms) == 0 {
		writeError(w, http.StatusBadRequest, "invalid_request", "title and targetPlatforms are required")
		return
	}

	platforms := make([]dbgen.Platform, 0, len(req.TargetPlatforms))
	for _, p := range req.TargetPlatforms {
		platforms = append(platforms, dbgen.Platform(p))
	}
	topic, err := h.q.CreateManualTopic(r.Context(), dbgen.CreateManualTopicParams{
		Title:           req.Title,
		Angle:           req.Angle,
		Summary:         req.Summary,
		TargetPlatforms: platforms,
		CorrelationID:   uuid.NullUUID{UUID: uuid.New(), Valid: true},
	})
	if err != nil {
		h.internalError(w, "create manual topic", err)
		return
	}
	writeJSON(w, http.StatusCreated, toAPITopic(&topic))
}

func (h *Server) GetTopic(w http.ResponseWriter, r *http.Request, topicId TopicId) {
	topic, err := h.q.GetTopic(r.Context(), topicId)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "not_found", "topic not found")
		return
	}
	if err != nil {
		h.internalError(w, "get topic", err)
		return
	}
	writeJSON(w, http.StatusOK, toAPITopic(&topic))
}

func toAPITopic(t *dbgen.Topic) Topic {
	var score *float32
	if t.LatestScore.Valid {
		if f, err := t.LatestScore.Float64Value(); err == nil && f.Valid {
			v := float32(f.Float64)
			score = &v
		}
	}
	platforms := make([]Platform, 0, len(t.TargetPlatforms))
	for _, p := range t.TargetPlatforms {
		platforms = append(platforms, Platform(p))
	}
	rawItemIds := t.RawItemIds
	if rawItemIds == nil {
		rawItemIds = []TopicId{}
	}
	return Topic{
		Id:              t.ID,
		Title:           t.Title,
		Angle:           t.Angle,
		Summary:         t.Summary,
		RawItemIds:      rawItemIds,
		TargetPlatforms: platforms,
		Status:          TopicStatus(t.Status),
		LatestScore:     score,
	}
}

func (h *Server) internalError(w http.ResponseWriter, op string, err error) {
	h.log.Error(op, "error", err)
	writeError(w, http.StatusInternalServerError, "internal", "internal server error")
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, Error{Code: code, Message: msg})
}
