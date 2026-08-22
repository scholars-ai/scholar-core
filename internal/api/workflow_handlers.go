package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/scholars-ai/scholar-core/internal/db/dbgen"
	"github.com/scholars-ai/scholar-core/internal/queue"
)

const workflowEventPageSize = int32(500)

func isWorkflowStreamPath(path string) bool {
	return strings.HasSuffix(path, "/stream")
}

func IsWorkflowStreamPath(path string) bool {
	return isWorkflowStreamPath(path)
}

func (h *Server) CreateWorkflowRun(w http.ResponseWriter, r *http.Request) {
	var req CreateWorkflowRunRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}

	runID := uuid.New()
	var created dbgen.WorkflowRun
	err := pgx.BeginFunc(r.Context(), h.pool, func(tx pgx.Tx) error {
		qtx := h.q.WithTx(tx)
		sourceIDs, err := workflowSourceIDs(r.Context(), qtx, req.SourceIds)
		if err != nil {
			return err
		}
		if len(sourceIDs) == 0 {
			return errNoWorkflowSources
		}

		metadata := map[string]any{"sourceCount": len(sourceIDs), "sourceIds": sourceIDs}
		if req.Metadata != nil {
			for key, value := range *req.Metadata {
				metadata[key] = value
			}
		}
		metadataJSON, err := json.Marshal(metadata)
		if err != nil {
			return err
		}
		created, err = qtx.CreateWorkflowRun(r.Context(), dbgen.CreateWorkflowRunParams{
			ID: runID, CorrelationID: runID, Mode: "cascade", StartNode: "source_fetch",
			Metadata: metadataJSON,
		})
		if err != nil {
			return err
		}

		messageIDs := make([]int64, 0, len(sourceIDs))
		for _, sourceID := range sourceIDs {
			messageID, err := queue.Send(r.Context(), tx, queue.SourceFetch,
				buildWorkflowSourcePayload(sourceID, runID),
				queue.WithCorrelation(runID), queue.WithTrigger("api"),
			)
			if err != nil {
				return err
			}
			messageIDs = append(messageIDs, messageID)
		}
		payload, err := json.Marshal(map[string]any{
			"sourceCount": len(sourceIDs), "messageIds": messageIDs,
		})
		if err != nil {
			return err
		}
		_, err = qtx.CreateWorkflowEvent(r.Context(), dbgen.CreateWorkflowEventParams{
			RunID: runID, NodeKey: "source_fetch", EventType: "run_created", Status: "queued",
			Message: "工作流已创建，采集任务已入队", Payload: payload,
		})
		return err
	})
	if errors.Is(err, errNoWorkflowSources) {
		writeError(w, http.StatusConflict, "no_enabled_sources", "没有可运行的已启用信源")
		return
	}
	if errors.Is(err, errInvalidWorkflowSource) {
		writeError(w, http.StatusBadRequest, "invalid_source", "sourceIds 包含不存在、已停用或已归档的信源")
		return
	}
	if err != nil {
		h.internalError(w, "create workflow run", err)
		return
	}
	writeJSON(w, http.StatusAccepted, workflowRunToAPI(created))
}

var (
	errNoWorkflowSources     = errors.New("workflow has no enabled sources")
	errInvalidWorkflowSource = errors.New("invalid workflow source")
)

func workflowSourceIDs(ctx context.Context, q *dbgen.Queries, requested *[]uuid.UUID) ([]uuid.UUID, error) {
	if requested == nil || len(*requested) == 0 {
		return q.ListEnabledSourceIDs(ctx)
	}
	seen := make(map[uuid.UUID]struct{}, len(*requested))
	result := make([]uuid.UUID, 0, len(*requested))
	for _, sourceID := range *requested {
		if _, duplicate := seen[sourceID]; duplicate {
			continue
		}
		source, err := q.GetSource(ctx, sourceID)
		if errors.Is(err, pgx.ErrNoRows) || err == nil && (!source.Enabled || source.ArchivedAt.Valid) {
			return nil, errInvalidWorkflowSource
		}
		if err != nil {
			return nil, err
		}
		seen[sourceID] = struct{}{}
		result = append(result, sourceID)
	}
	return result, nil
}

func buildWorkflowSourcePayload(sourceID, runID uuid.UUID) map[string]any {
	return map[string]any{
		"sourceId": sourceID.String(), "cascade": true, "workflowRunId": runID.String(),
	}
}

func (h *Server) ListWorkflowRuns(w http.ResponseWriter, r *http.Request, params ListWorkflowRunsParams) {
	limit := int32(30)
	if params.Limit != nil {
		limit = int32(*params.Limit)
	}
	rows, err := h.q.ListWorkflowRuns(r.Context(), limit)
	if err != nil {
		h.internalError(w, "list workflow runs", err)
		return
	}
	items := make([]WorkflowRun, 0, len(rows))
	for _, row := range rows {
		items = append(items, workflowRunToAPI(row))
	}
	writeJSON(w, http.StatusOK, WorkflowRunList{Items: items})
}

func (h *Server) GetWorkflowRun(w http.ResponseWriter, r *http.Request, runID uuid.UUID) {
	run, err := h.q.GetWorkflowRun(r.Context(), runID)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "not_found", "workflow run not found")
		return
	}
	if err != nil {
		h.internalError(w, "get workflow run", err)
		return
	}
	events, err := h.q.ListWorkflowEvents(r.Context(), dbgen.ListWorkflowEventsParams{
		RunID: runID, Sequence: pgtype.Int8{Int64: 0, Valid: true}, Limit: workflowEventPageSize,
	})
	if err != nil {
		h.internalError(w, "list workflow events", err)
		return
	}
	artifacts, err := h.q.ListWorkflowArtifacts(r.Context(), runID)
	if err != nil {
		h.internalError(w, "list workflow artifacts", err)
		return
	}
	writeJSON(w, http.StatusOK, workflowRunDetailToAPI(run, events, artifacts))
}

func (h *Server) ListWorkflowEvents(w http.ResponseWriter, r *http.Request, runID uuid.UUID, params ListWorkflowEventsParams) {
	if !h.workflowRunExists(w, r, runID) {
		return
	}
	after := int64(0)
	if params.After != nil {
		after = *params.After
	}
	rows, err := h.workflowEventsAfter(r, runID, after)
	if err != nil {
		h.internalError(w, "list workflow events", err)
		return
	}
	writeJSON(w, http.StatusOK, workflowEventsToAPI(rows))
}

func (h *Server) ListWorkflowArtifacts(w http.ResponseWriter, r *http.Request, runID uuid.UUID) {
	if !h.workflowRunExists(w, r, runID) {
		return
	}
	rows, err := h.q.ListWorkflowArtifacts(r.Context(), runID)
	if err != nil {
		h.internalError(w, "list workflow artifacts", err)
		return
	}
	writeJSON(w, http.StatusOK, workflowArtifactsToAPI(rows))
}

func (h *Server) StreamWorkflowRun(w http.ResponseWriter, r *http.Request, runID uuid.UUID) {
	if !h.workflowRunExists(w, r, runID) {
		return
	}
	controller := http.NewResponseController(w)
	if err := controller.Flush(); err != nil {
		writeError(w, http.StatusInternalServerError, "streaming_unsupported", "streaming is not supported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache, no-transform")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	after := lastEventSequence(r.Header.Get("Last-Event-ID"))
	ticker := time.NewTicker(time.Second)
	heartbeat := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	defer heartbeat.Stop()

	for {
		rows, err := h.workflowEventsAfter(r, runID, after)
		if err != nil {
			h.log.Warn("stream workflow events", "run_id", runID, "error", err)
			return
		}
		for _, event := range workflowEventsToAPI(rows) {
			body, err := json.Marshal(event)
			if err != nil {
				return
			}
			fmt.Fprintf(w, "id: %d\nevent: workflow\ndata: %s\n\n", event.Sequence, body)
			after = event.Sequence
		}
		if len(rows) > 0 {
			_ = controller.Flush()
		}

		select {
		case <-r.Context().Done():
			return
		case <-heartbeat.C:
			fmt.Fprint(w, ": keep-alive\n\n")
			_ = controller.Flush()
		case <-ticker.C:
		}
	}
}

func (h *Server) workflowEventsAfter(r *http.Request, runID uuid.UUID, after int64) ([]dbgen.WorkflowEvent, error) {
	return h.q.ListWorkflowEvents(r.Context(), dbgen.ListWorkflowEventsParams{
		RunID: runID, Sequence: pgtype.Int8{Int64: after, Valid: true}, Limit: workflowEventPageSize,
	})
}

func (h *Server) workflowRunExists(w http.ResponseWriter, r *http.Request, runID uuid.UUID) bool {
	_, err := h.q.GetWorkflowRun(r.Context(), runID)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "not_found", "workflow run not found")
		return false
	}
	if err != nil {
		h.internalError(w, "get workflow run", err)
		return false
	}
	return true
}

func lastEventSequence(value string) int64 {
	sequence, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil || sequence < 0 {
		return 0
	}
	return sequence
}

func reduceWorkflowNodeStatus(events []dbgen.WorkflowEvent, nodeKey string) string {
	status := "idle"
	for _, event := range events {
		if event.NodeKey == nodeKey {
			status = event.Status
		}
	}
	return status
}

func workflowRunToAPI(row dbgen.WorkflowRun) WorkflowRun {
	metadata := jsonObject(row.Metadata)
	return WorkflowRun{
		Id: row.ID, CorrelationId: row.CorrelationID, Mode: WorkflowRunMode(row.Mode),
		StartNode: WorkflowRunStartNode(row.StartNode), Status: WorkflowRunStatus(row.Status),
		ErrorMessage: optionalText(row.ErrorMessage), Metadata: &metadata,
		CreatedAt: row.CreatedAt.Time, StartedAt: optionalTime(row.StartedAt),
		CompletedAt: optionalTime(row.CompletedAt),
	}
}

func workflowRunDetailToAPI(run dbgen.WorkflowRun, events []dbgen.WorkflowEvent, artifacts []dbgen.WorkflowArtifact) WorkflowRunDetail {
	base := workflowRunToAPI(run)
	return WorkflowRunDetail{
		Id: base.Id, CorrelationId: base.CorrelationId, Mode: WorkflowRunDetailMode(base.Mode),
		StartNode: WorkflowRunDetailStartNode(base.StartNode), Status: WorkflowRunDetailStatus(base.Status),
		ErrorMessage: base.ErrorMessage, Metadata: base.Metadata, CreatedAt: base.CreatedAt,
		StartedAt: base.StartedAt, CompletedAt: base.CompletedAt,
		Events: workflowEventsToAPI(events), Artifacts: workflowArtifactsToAPI(artifacts),
	}
}

func workflowEventsToAPI(rows []dbgen.WorkflowEvent) []WorkflowEvent {
	items := make([]WorkflowEvent, 0, len(rows))
	for _, row := range rows {
		var agentRunID *uuid.UUID
		if row.AgentRunID.Valid {
			value := row.AgentRunID.UUID
			agentRunID = &value
		}
		items = append(items, WorkflowEvent{
			Id: row.ID, RunId: row.RunID, Sequence: row.Sequence.Int64, NodeKey: row.NodeKey,
			EventType: row.EventType, Status: row.Status, Message: row.Message,
			AgentRunId: agentRunID, Payload: jsonObject(row.Payload), OccurredAt: row.OccurredAt.Time,
		})
	}
	return items
}

func workflowArtifactsToAPI(rows []dbgen.WorkflowArtifact) []WorkflowArtifact {
	items := make([]WorkflowArtifact, 0, len(rows))
	for _, row := range rows {
		items = append(items, WorkflowArtifact{
			Id: row.ID, RunId: row.RunID, NodeKey: row.NodeKey, ArtifactType: row.ArtifactType,
			ArtifactId: row.ArtifactID, Title: row.Title, Metadata: jsonObject(row.Metadata),
			CreatedAt: row.CreatedAt.Time,
		})
	}
	return items
}

func jsonObject(raw []byte) map[string]any {
	value := map[string]any{}
	_ = json.Unmarshal(raw, &value)
	return value
}

func optionalText(value pgtype.Text) *string {
	if !value.Valid {
		return nil
	}
	return &value.String
}

func optionalTime(value pgtype.Timestamptz) *time.Time {
	if !value.Valid {
		return nil
	}
	return &value.Time
}
