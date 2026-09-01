package api

import (
	"context"
	"crypto/sha256"
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
	"github.com/scholars-ai/scholar-core/internal/workflow"
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

	sourceIDs, err := workflowSourceIDs(r.Context(), h.q, req.SourceIds)
	if errors.Is(err, errInvalidWorkflowSource) {
		writeError(w, http.StatusBadRequest, "invalid_source", "sourceIds 包含不存在、已停用或已归档的信源")
		return
	}
	if err != nil {
		h.internalError(w, "resolve workflow sources", err)
		return
	}
	metadata := map[string]any{}
	if req.Metadata != nil {
		metadata = *req.Metadata
	}
	created, err := workflow.New(h.pool, h.log).CreateContentRun(r.Context(), workflow.CreateOptions{
		TriggerType: "manual", SourceIDs: sourceIDs, Metadata: metadata,
	})
	if err != nil {
		h.internalError(w, "create workflow run", err)
		return
	}
	writeJSON(w, http.StatusAccepted, workflowRunToAPI(created))
}

var (
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
	nodeRuns, err := h.q.ListWorkflowNodeRuns(r.Context(), runID)
	if err != nil {
		h.internalError(w, "list workflow node runs", err)
		return
	}
	decisions, err := h.q.ListWorkflowDecisions(r.Context(), dbgen.ListWorkflowDecisionsParams{
		RunID: runID, Column2: "", Column3: "",
	})
	if err != nil {
		h.internalError(w, "list workflow decisions", err)
		return
	}
	writeJSON(w, http.StatusOK, workflowRunDetailToAPI(run, events, artifacts, nodeRuns, decisions))
}

// ListWorkflowNodeDecisions returns the immutable item-level decisions for one node.
func (h *Server) ListWorkflowNodeDecisions(w http.ResponseWriter, r *http.Request, runID uuid.UUID, nodeKey WorkflowNodeKey, params ListWorkflowNodeDecisionsParams) {
	if !h.workflowRunExists(w, r, runID) {
		return
	}
	valid := map[WorkflowNodeKey]bool{
		"source_fetch": true, "topic_scout": true, "topic_evaluate": true,
		"article_write": true, "article_evaluate": true, "human_review": true,
	}
	if !valid[nodeKey] {
		writeError(w, http.StatusBadRequest, "invalid_node", "unknown workflow node")
		return
	}
	dec := ""
	if params.Decision != nil {
		dec = string(*params.Decision)
	}
	nodeRuns, err := h.q.ListWorkflowNodeRuns(r.Context(), runID)
	if err != nil {
		h.internalError(w, "list workflow node runs", err)
		return
	}
	var nodeRunID uuid.UUID
	for _, nodeRun := range nodeRuns {
		if nodeRun.NodeKey == string(nodeKey) {
			nodeRunID = nodeRun.ID
			break
		}
	}
	if nodeRunID == uuid.Nil {
		writeJSON(w, http.StatusOK, []WorkflowItemDecision{})
		return
	}
	rows, err := h.q.ListWorkflowDecisions(r.Context(), dbgen.ListWorkflowDecisionsParams{
		RunID: runID, Column2: nodeRunID, Column3: dec,
	})
	if err != nil {
		h.internalError(w, "list workflow node decisions", err)
		return
	}
	writeJSON(w, http.StatusOK, workflowDecisionsToAPI(rows))
}

// ReplayWorkflowRun creates an immutable child run. Execution wiring is handled by the workflow scheduler.
func (h *Server) ReplayWorkflowRun(w http.ResponseWriter, r *http.Request, runID uuid.UUID) {
	var req ReplayWorkflowRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}
	if req.ReplayFromNode == "" || req.ReplayScope.Mode == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "replayFromNode and replayScope are required")
		return
	}
	valid := map[WorkflowNodeKey]bool{
		"source_fetch": true, "topic_scout": true, "topic_evaluate": true,
		"article_write": true, "article_evaluate": true, "human_review": true,
	}
	if !valid[req.ReplayFromNode] {
		writeError(w, http.StatusBadRequest, "invalid_node", "unknown workflow node")
		return
	}
	child, err := workflow.New(h.pool, h.log).CreateReplay(r.Context(), runID, replayScopeMap(req.ReplayScope), string(req.ReplayFromNode), req.Reason, configOverridesMap(req.ConfigOverrides))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			writeError(w, http.StatusNotFound, "not_found", "workflow run not found")
			return
		}
		writeError(w, http.StatusBadRequest, "invalid_replay", err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, workflowRunToAPI(child))
}

func (h *Server) CompareWorkflowRuns(w http.ResponseWriter, r *http.Request, runID uuid.UUID) {
	var req CompareWorkflowRunsJSONBody
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", err.Error())
		return
	}
	base, err := h.q.GetWorkflowRun(r.Context(), runID)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "not_found", "base workflow run not found")
		return
	}
	if err != nil {
		h.internalError(w, "get base workflow run", err)
		return
	}
	other, err := h.q.GetWorkflowRun(r.Context(), req.OtherRunId)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "not_found", "other workflow run not found")
		return
	}
	if err != nil {
		h.internalError(w, "get other workflow run", err)
		return
	}
	baseDecisions, err := h.q.ListWorkflowDecisions(r.Context(), dbgen.ListWorkflowDecisionsParams{RunID: runID, Column2: "", Column3: ""})
	if err != nil {
		h.internalError(w, "list base workflow decisions", err)
		return
	}
	otherDecisions, err := h.q.ListWorkflowDecisions(r.Context(), dbgen.ListWorkflowDecisionsParams{RunID: req.OtherRunId, Column2: "", Column3: ""})
	if err != nil {
		h.internalError(w, "list other workflow decisions", err)
		return
	}
	baseNodes, err := h.q.ListWorkflowNodeRuns(r.Context(), runID)
	if err != nil {
		h.internalError(w, "list base workflow nodes", err)
		return
	}
	otherNodes, err := h.q.ListWorkflowNodeRuns(r.Context(), req.OtherRunId)
	if err != nil {
		h.internalError(w, "list other workflow nodes", err)
		return
	}
	baseArtifacts, err := h.q.ListWorkflowArtifacts(r.Context(), runID)
	if err != nil {
		h.internalError(w, "list base workflow artifacts", err)
		return
	}
	otherArtifacts, err := h.q.ListWorkflowArtifacts(r.Context(), req.OtherRunId)
	if err != nil {
		h.internalError(w, "list other workflow artifacts", err)
		return
	}
	baseUsage, err := h.workflowUsage(r.Context(), base.CorrelationID)
	if err != nil {
		h.internalError(w, "list base workflow usage", err)
		return
	}
	otherUsage, err := h.workflowUsage(r.Context(), other.CorrelationID)
	if err != nil {
		h.internalError(w, "list other workflow usage", err)
		return
	}
	writeJSON(w, http.StatusOK, compareWorkflowRunsWithUsage(base, other, baseNodes, otherNodes, baseArtifacts, otherArtifacts, baseDecisions, otherDecisions, baseUsage, otherUsage))
}

func replayScopeMap(scope ReplayScope) map[string]any {
	result := map[string]any{"mode": string(scope.Mode)}
	if scope.ItemIds != nil {
		ids := make([]string, 0, len(*scope.ItemIds))
		for _, id := range *scope.ItemIds {
			ids = append(ids, id.String())
		}
		result["itemIds"] = ids
	}
	if scope.UseCurrentInput != nil {
		result["useCurrentInput"] = *scope.UseCurrentInput
	}
	return result
}

func configOverridesMap(overrides *WorkflowConfigOverrides) map[string]any {
	if overrides == nil {
		return nil
	}
	raw, err := json.Marshal(overrides)
	if err != nil {
		return nil
	}
	result := map[string]any{}
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil
	}
	return result
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

// GetWorkflowSnapshot returns an immutable node input/output/config snapshot.
// The run id is part of the lookup so a snapshot cannot be read through an
// unrelated workflow run, and the checksum is verified before exposing data.
func (h *Server) GetWorkflowSnapshot(w http.ResponseWriter, r *http.Request, runID uuid.UUID, snapshotID uuid.UUID) {
	row, err := h.q.GetWorkflowSnapshotForRun(r.Context(), dbgen.GetWorkflowSnapshotForRunParams{
		ID: snapshotID, RunID: runID,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "not_found", "workflow snapshot not found")
		return
	}
	if err != nil {
		h.internalError(w, "get workflow snapshot", err)
		return
	}
	if !workflowSnapshotChecksumValid(row.Payload, row.Sha256) {
		writeError(w, http.StatusInternalServerError, "snapshot_corrupt", "workflow snapshot checksum mismatch")
		return
	}
	writeJSON(w, http.StatusOK, workflowSnapshotToAPI(row))
}

func workflowSnapshotChecksumValid(payload []byte, expected string) bool {
	hash := sha256.Sum256(payload)
	return fmt.Sprintf("%x", hash[:]) == expected
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
	out := WorkflowRun{
		Id: row.ID, CorrelationId: row.CorrelationID, Mode: WorkflowRunMode(row.Mode),
		StartNode: WorkflowNodeKey(row.StartNode), Status: WorkflowRunStatus(row.Status),
		ErrorMessage: optionalText(row.ErrorMessage), Metadata: &metadata,
		CreatedAt: row.CreatedAt.Time, StartedAt: optionalTime(row.StartedAt),
		CompletedAt: optionalTime(row.CompletedAt), TriggerType: WorkflowTriggerType(row.TriggerType),
	}
	if row.ParentRunID.Valid {
		v := row.ParentRunID.UUID
		out.ParentRunId = &v
	}
	if row.ReplayFromNode.Valid {
		v := WorkflowNodeKey(row.ReplayFromNode.String)
		out.ReplayFromNode = &v
	}
	replayScope := jsonObject(row.ReplayScope)
	out.ReplayScope = &replayScope
	if row.InputSnapshotID.Valid {
		v := row.InputSnapshotID.UUID
		out.InputSnapshotId = &v
	}
	if row.ConfigSnapshotID.Valid {
		v := row.ConfigSnapshotID.UUID
		out.ConfigSnapshotId = &v
	}
	summary := jsonObject(row.Summary)
	out.Summary = &summary
	return out
}

func workflowRunDetailToAPI(run dbgen.WorkflowRun, events []dbgen.WorkflowEvent, artifacts []dbgen.WorkflowArtifact, nodeRuns []dbgen.WorkflowNodeRun, decisions []dbgen.WorkflowItemDecision) WorkflowRunDetail {
	base := workflowRunToAPI(run)
	return WorkflowRunDetail{
		Id: base.Id, CorrelationId: base.CorrelationId, Mode: base.Mode,
		StartNode: base.StartNode, Status: base.Status,
		ErrorMessage: base.ErrorMessage, Metadata: base.Metadata, CreatedAt: base.CreatedAt,
		StartedAt: base.StartedAt, CompletedAt: base.CompletedAt,
		TriggerType: base.TriggerType, ParentRunId: base.ParentRunId, ReplayFromNode: base.ReplayFromNode,
		ReplayScope: base.ReplayScope, InputSnapshotId: base.InputSnapshotId, ConfigSnapshotId: base.ConfigSnapshotId,
		Summary: base.Summary, Events: workflowEventsToAPI(events), Artifacts: workflowArtifactsToAPI(artifacts),
		NodeRuns: workflowNodeRunsToAPI(nodeRuns), Decisions: workflowDecisionsToAPI(decisions),
	}
}

func workflowNodeRunsToAPI(rows []dbgen.WorkflowNodeRun) []WorkflowNodeRun {
	items := make([]WorkflowNodeRun, 0, len(rows))
	for _, row := range rows {
		items = append(items, WorkflowNodeRun{Id: row.ID, RunId: row.RunID, NodeKey: WorkflowNodeKey(row.NodeKey), Status: WorkflowNodeStatus(row.Status),
			InputSnapshotId: nullUUIDPtr(row.InputSnapshotID), OutputSnapshotId: nullUUIDPtr(row.OutputSnapshotID), ConfigSnapshot: jsonObject(row.ConfigSnapshot), Counts: jsonObject(row.Counts),
			CreatedAt: row.CreatedAt.Time, StartedAt: optionalTime(row.StartedAt), CompletedAt: optionalTime(row.CompletedAt)})
	}
	return items
}

func workflowDecisionsToAPI(rows []dbgen.WorkflowItemDecision) []WorkflowItemDecision {
	items := make([]WorkflowItemDecision, 0, len(rows))
	for _, row := range rows {
		var scores *map[string]float32
		if len(row.DimensionScores) > 0 {
			var value map[string]float32
			if json.Unmarshal(row.DimensionScores, &value) == nil {
				scores = &value
			}
		}
		item := WorkflowItemDecision{Id: row.ID, RunId: row.RunID, NodeRunId: row.NodeRunID, ItemId: row.ItemID, ItemType: WorkflowItemType(row.ItemType), Decision: WorkflowDecision(row.Decision), ReasonCode: row.ReasonCode, Reason: row.Reason,
			DimensionScores: scores, InputRefs: jsonObject(row.InputRefs), EvidenceRefs: jsonObject(row.EvidenceRefs), CreatedAt: row.CreatedAt.Time}
		if row.TotalScore.Valid {
			v := numericFloat32(row.TotalScore)
			item.TotalScore = &v
		}
		if row.Threshold.Valid {
			v := numericFloat32(row.Threshold)
			item.Threshold = &v
		}
		if row.WeightVersion.Valid {
			v := int(row.WeightVersion.Int32)
			item.WeightVersion = &v
		}
		if row.RubricVersion.Valid {
			v := row.RubricVersion.String
			item.RubricVersion = &v
		}
		if row.AgentRunID.Valid {
			v := row.AgentRunID.UUID
			item.AgentRunId = &v
		}
		if row.TraceID.Valid {
			v := row.TraceID.String
			item.TraceId = &v
		}
		items = append(items, item)
	}
	return items
}

func nullUUIDPtr(value uuid.NullUUID) *uuid.UUID {
	if value.Valid {
		v := value.UUID
		return &v
	}
	return nil
}

func compareWorkflowRuns(base, other dbgen.WorkflowRun, baseNodes, otherNodes []dbgen.WorkflowNodeRun, baseArtifacts, otherArtifacts []dbgen.WorkflowArtifact, baseDecisions, otherDecisions []dbgen.WorkflowItemDecision) WorkflowRunComparison {
	return compareWorkflowRunsWithUsage(base, other, baseNodes, otherNodes, baseArtifacts, otherArtifacts, baseDecisions, otherDecisions, nil, nil)
}

type workflowStageUsage struct {
	tokens    int
	cost      float32
	hasTokens bool
	hasCost   bool
}

func (h *Server) workflowUsage(ctx context.Context, correlationID uuid.UUID) (map[string]workflowStageUsage, error) {
	rows, err := h.pool.Query(ctx, `
		select replace(job_type, '.', '_') as node_key,
		       coalesce(sum(coalesce(tokens_in, 0) + coalesce(tokens_out, 0))
		           filter (where tokens_in is not null or tokens_out is not null), 0)::bigint,
		       coalesce(sum(cost_usd) filter (where cost_usd is not null), 0)::numeric,
		       coalesce(bool_or(tokens_in is not null or tokens_out is not null), false),
		       coalesce(bool_or(cost_usd is not null), false)
		from agent_runs
		where correlation_id = $1
		  and job_type in ('source.fetch', 'topic.scout', 'topic.evaluate', 'article.write', 'article.evaluate')
		group by job_type`, correlationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	usage := make(map[string]workflowStageUsage)
	for rows.Next() {
		var node string
		var tokens int64
		var cost pgtype.Numeric
		var hasTokens, hasCost bool
		if err := rows.Scan(&node, &tokens, &cost, &hasTokens, &hasCost); err != nil {
			return nil, err
		}
		usage[node] = workflowStageUsage{tokens: int(tokens), cost: numericFloat32(cost), hasTokens: hasTokens, hasCost: hasCost}
	}
	return usage, rows.Err()
}

func compareWorkflowRunsWithUsage(base, other dbgen.WorkflowRun, baseNodes, otherNodes []dbgen.WorkflowNodeRun, baseArtifacts, otherArtifacts []dbgen.WorkflowArtifact, baseDecisions, otherDecisions []dbgen.WorkflowItemDecision, baseUsage, otherUsage map[string]workflowStageUsage) WorkflowRunComparison {
	stages := map[string]WorkflowStageComparison{}
	reasonCounts := map[string]interface{}{}
	for _, node := range []string{"source_fetch", "topic_scout", "topic_evaluate", "article_write", "article_evaluate", "human_review"} {
		baseMetrics := workflowStageMetrics(node, findWorkflowNode(baseNodes, node), baseDecisions, reasonCounts, "base")
		otherMetrics := workflowStageMetrics(node, findWorkflowNode(otherNodes, node), otherDecisions, reasonCounts, "other")
		applyWorkflowUsage(&baseMetrics, node, findWorkflowNode(baseNodes, node), baseUsage)
		applyWorkflowUsage(&otherMetrics, node, findWorkflowNode(otherNodes, node), otherUsage)
		stages[node] = WorkflowStageComparison{Base: baseMetrics, Other: otherMetrics}
	}
	artifacts := compareArtifacts(baseArtifacts, otherArtifacts)
	cost := map[string]interface{}{
		"base":  workflowRunCost(base, baseUsage),
		"other": workflowRunCost(other, otherUsage),
	}
	return WorkflowRunComparison{
		BaseRunId: base.ID, OtherRunId: other.ID,
		SameInput: base.InputSnapshotID.Valid && other.InputSnapshotID.Valid && base.InputSnapshotID.UUID == other.InputSnapshotID.UUID,
		Stages:    stages, ReasonCounts: reasonCounts, Artifacts: &artifacts, Cost: &cost,
	}
}

func applyWorkflowUsage(metrics *WorkflowStageMetrics, node string, nodeRun dbgen.WorkflowNodeRun, usage map[string]workflowStageUsage) {
	if nodeRun.StartedAt.Valid && nodeRun.CompletedAt.Valid {
		seconds := float32(nodeRun.CompletedAt.Time.Sub(nodeRun.StartedAt.Time).Seconds())
		if seconds >= 0 {
			metrics.DurationSeconds = &seconds
		}
	}
	stage, ok := usage[node]
	if !ok {
		return
	}
	if stage.hasTokens {
		value := stage.tokens
		metrics.TokenCount = &value
	}
	if stage.hasCost {
		value := stage.cost
		metrics.Cost = &value
	}
}

func workflowRunCost(run dbgen.WorkflowRun, usage map[string]workflowStageUsage) map[string]interface{} {
	result := map[string]interface{}{}
	var tokens int
	var cost float32
	knownTokens, knownCost := false, false
	for _, stage := range usage {
		if stage.hasTokens {
			tokens += stage.tokens
			knownTokens = true
		}
		if stage.hasCost {
			cost += stage.cost
			knownCost = true
		}
	}
	if knownTokens {
		result["tokenCount"] = tokens
	}
	if knownCost {
		result["costUsd"] = cost
	}
	if run.StartedAt.Valid {
		end := run.CompletedAt
		if !end.Valid {
			end = run.UpdatedAt
		}
		if end.Valid {
			result["durationSeconds"] = end.Time.Sub(run.StartedAt.Time).Seconds()
		}
	}
	return result
}

func findWorkflowNode(nodes []dbgen.WorkflowNodeRun, key string) dbgen.WorkflowNodeRun {
	for _, node := range nodes {
		if node.NodeKey == key {
			return node
		}
	}
	return dbgen.WorkflowNodeRun{}
}

func workflowStageMetrics(node string, nodeRun dbgen.WorkflowNodeRun, decisions []dbgen.WorkflowItemDecision, reasonCounts map[string]interface{}, set string) WorkflowStageMetrics {
	counts := jsonObject(nodeRun.Counts)
	metrics := WorkflowStageMetrics{
		InputCount: workflowIntPtr(jsonInt(counts, "input")), OutputCount: workflowIntPtr(jsonInt(counts, "output")),
		Accepted: workflowIntPtr(jsonInt(counts, "accepted")), Rejected: workflowIntPtr(jsonInt(counts, "rejected")),
		Skipped: workflowIntPtr(jsonInt(counts, "skipped")), Failed: workflowIntPtr(jsonInt(counts, "failed")),
	}
	if *metrics.InputCount > 0 {
		value := float32(*metrics.Accepted) / float32(*metrics.InputCount)
		metrics.PassRate = &value
	}
	if nodeRun.ID == uuid.Nil {
		return metrics
	}
	var scores []float32
	for _, row := range decisions {
		if row.NodeRunID != nodeRun.ID {
			continue
		}
		if row.ReasonCode != "" {
			key := set + ":" + node + ":" + row.ReasonCode
			current, _ := reasonCounts[key].(int)
			reasonCounts[key] = current + 1
		}
		if row.TotalScore.Valid {
			scores = append(scores, numericFloat32(row.TotalScore))
		}
	}
	if len(scores) > 0 {
		histogram := map[string]interface{}{"0-59": 0, "60-79": 0, "80-100": 0}
		for _, score := range scores {
			bucket := "80-100"
			if score < 60 {
				bucket = "0-59"
			} else if score < 80 {
				bucket = "60-79"
			}
			histogram[bucket] = histogram[bucket].(int) + 1
		}
		metrics.ScoreDistribution = &histogram
	}
	return metrics
}

func jsonInt(value map[string]interface{}, key string) int {
	if number, ok := value[key].(float64); ok {
		return int(number)
	}
	if number, ok := value[key].(int); ok {
		return number
	}
	return 0
}

func workflowIntPtr(value int) *int { return &value }

func compareArtifacts(base, other []dbgen.WorkflowArtifact) map[string]interface{} {
	baseSet := make(map[string]struct{}, len(base))
	otherSet := make(map[string]struct{}, len(other))
	for _, item := range base {
		baseSet[item.ArtifactType+":"+item.ArtifactID.String()] = struct{}{}
	}
	for _, item := range other {
		otherSet[item.ArtifactType+":"+item.ArtifactID.String()] = struct{}{}
	}
	added, removed := 0, 0
	for key := range otherSet {
		if _, ok := baseSet[key]; !ok {
			added++
		}
	}
	for key := range baseSet {
		if _, ok := otherSet[key]; !ok {
			removed++
		}
	}
	return map[string]interface{}{"baseCount": len(baseSet), "otherCount": len(otherSet), "added": added, "removed": removed}
}

func runComparisonSummary(run dbgen.WorkflowRun) map[string]interface{} {
	result := map[string]interface{}{"status": run.Status}
	if run.StartedAt.Valid {
		end := run.CompletedAt
		if !end.Valid {
			end = run.UpdatedAt
		}
		if end.Valid {
			result["durationSeconds"] = end.Time.Sub(run.StartedAt.Time).Seconds()
		}
	}
	if len(run.Summary) > 0 {
		result["summary"] = jsonObject(run.Summary)
	}
	return result
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
		item := WorkflowArtifact{
			Id: row.ID, RunId: row.RunID, NodeKey: row.NodeKey, ArtifactType: row.ArtifactType,
			ArtifactId: row.ArtifactID, Title: row.Title, Metadata: jsonObject(row.Metadata),
			CreatedAt: row.CreatedAt.Time,
		}
		if row.ParentArtifactID.Valid {
			value := row.ParentArtifactID.UUID
			item.ParentArtifactId = &value
		}
		if row.SnapshotID.Valid {
			value := row.SnapshotID.UUID
			item.SnapshotId = &value
		}
		items = append(items, item)
	}
	return items
}

func workflowSnapshotToAPI(row dbgen.WorkflowSnapshot) WorkflowSnapshot {
	return WorkflowSnapshot{
		Id: row.ID, RunId: row.RunID, Kind: WorkflowSnapshotKind(row.Kind),
		Payload: jsonObject(row.Payload), Sha256: row.Sha256, CreatedAt: row.CreatedAt.Time,
	}
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
