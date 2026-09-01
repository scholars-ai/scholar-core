// Package workflow owns the fixed content-production workflow runtime.
//
// The domain state machines remain in Core's pipeline package. This package
// only owns run boundaries, snapshots, barriers, queue fan-out and run-level
// accounting required by SPEC-010.
package workflow

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/scholars-ai/scholar-core/internal/db/dbgen"
	"github.com/scholars-ai/scholar-core/internal/queue"
)

const (
	workflowMode       = "content_production"
	defaultTopicPass   = 60.0
	workflowVersion    = "content-production@v1"
	maxReconcileRuns   = 100
	workflowRunTimeout = 72 * time.Hour
)

var nodeOrder = []string{
	"source_fetch",
	"topic_scout",
	"topic_evaluate",
	"article_write",
	"article_evaluate",
	"human_review",
}

type Runtime struct {
	pool *pgxpool.Pool
	q    *dbgen.Queries
	log  *slog.Logger
}

func New(pool *pgxpool.Pool, log *slog.Logger) *Runtime {
	return &Runtime{pool: pool, q: dbgen.New(pool), log: log}
}

type CreateOptions struct {
	TriggerType     string
	StartNode       string
	SourceIDs       []uuid.UUID
	Metadata        map[string]any
	ParentRunID     *uuid.UUID
	ReplayFromNode  *string
	ReplayScope     map[string]any
	ConfigOverrides map[string]any
	InputSnapshotID *uuid.UUID
	ReplayKey       string
	ScheduleRunID   *uuid.UUID
}

// CreateContentRun creates a run and its source fan-out atomically. An empty
// source set is a valid run and is completed_empty with a diagnostic summary.
func (rt *Runtime) CreateContentRun(ctx context.Context, opts CreateOptions) (dbgen.WorkflowRun, error) {
	var err error
	if opts, err = normalizeCreateOptions(opts, "manual"); err != nil {
		return dbgen.WorkflowRun{}, err
	}
	runID := uuid.New()
	err = pgx.BeginFunc(ctx, rt.pool, func(tx pgx.Tx) error {
		return rt.createContentRunTx(ctx, tx, runID, opts)
	})
	if err != nil {
		return dbgen.WorkflowRun{}, err
	}
	return rt.q.GetWorkflowRun(ctx, runID)
}

// CreateScheduledContentRun atomically records the scheduler slot and creates
// the corresponding WorkflowRun and initial queue fan-out. created=false means
// this interval was already claimed by another scheduler instance.
func (rt *Runtime) CreateScheduledContentRun(ctx context.Context, opts CreateOptions, scheduleKey string, plannedAt time.Time, note string) (run dbgen.WorkflowRun, created bool, err error) {
	if opts, err = normalizeCreateOptions(opts, "scheduled"); err != nil {
		return dbgen.WorkflowRun{}, false, err
	}
	runID := uuid.New()
	err = pgx.BeginFunc(ctx, rt.pool, func(tx pgx.Tx) error {
		qtx := dbgen.New(tx)
		scheduleID, scheduleErr := qtx.RecordScheduleRun(ctx, dbgen.RecordScheduleRunParams{
			ScheduleKey: scheduleKey,
			PlannedAt:   pgtype.Timestamptz{Time: plannedAt, Valid: true},
			Queue:       string(queue.SourceFetch),
			Note:        pgtype.Text{String: note, Valid: note != ""},
		})
		if errors.Is(scheduleErr, pgx.ErrNoRows) {
			return errScheduleAlreadyClaimed
		}
		if scheduleErr != nil {
			return scheduleErr
		}
		opts.ScheduleRunID = &scheduleID
		return rt.createContentRunTx(ctx, tx, runID, opts)
	})
	if errors.Is(err, errScheduleAlreadyClaimed) {
		return dbgen.WorkflowRun{}, false, nil
	}
	if err != nil {
		return dbgen.WorkflowRun{}, false, err
	}
	run, err = rt.q.GetWorkflowRun(ctx, runID)
	return run, err == nil, err
}

var errScheduleAlreadyClaimed = errors.New("workflow schedule interval already claimed")

func normalizeCreateOptions(opts CreateOptions, defaultTrigger string) (CreateOptions, error) {
	if strings.TrimSpace(opts.TriggerType) == "" {
		opts.TriggerType = defaultTrigger
	}
	if strings.TrimSpace(opts.StartNode) == "" {
		opts.StartNode = nodeOrder[0]
	}
	if err := validateNode(opts.StartNode); err != nil {
		return CreateOptions{}, err
	}
	return opts, nil
}

func (rt *Runtime) createContentRunTx(ctx context.Context, tx pgx.Tx, runID uuid.UUID, opts CreateOptions) error {
	qtx := dbgen.New(tx)
	metadata := cloneMap(opts.Metadata)
	metadata["sourceCount"] = len(opts.SourceIDs)
	metadata["sourceIds"] = opts.SourceIDs
	metadata["workflowVersion"] = workflowVersion
	if opts.ScheduleRunID != nil {
		metadata["scheduleRunId"] = opts.ScheduleRunID
	}
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	if _, err := qtx.CreateWorkflowRun(ctx, dbgen.CreateWorkflowRunParams{
		ID: runID, CorrelationID: runID, Mode: workflowMode,
		StartNode: opts.StartNode, Metadata: metadataJSON,
	}); err != nil {
		return err
	}
	if err := rt.updateRunMetadata(ctx, tx, runID, opts); err != nil {
		return err
	}
	definitionID, err := rt.createSnapshot(ctx, qtx, runID, "definition", definitionPayload())
	if err != nil {
		return err
	}
	_ = definitionID
	var inputID uuid.UUID
	if opts.InputSnapshotID != nil {
		inputID = *opts.InputSnapshotID
	} else {
		inputPayload, _ := json.Marshal(map[string]any{
			"sourceIds": opts.SourceIDs, "triggerType": opts.TriggerType,
		})
		inputID, err = rt.createSnapshot(ctx, qtx, runID, "input", inputPayload)
		if err != nil {
			return err
		}
	}
	configPayload, _ := json.Marshal(map[string]any{
		"workflowVersion": workflowVersion, "triggerType": opts.TriggerType,
		"overrides": cloneMap(opts.ConfigOverrides),
	})
	configID, err := rt.createSnapshot(ctx, qtx, runID, "config", configPayload)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `update workflow_runs
            set input_snapshot_id = $2, config_snapshot_id = $3, updated_at = now()
            where id = $1`, runID, inputID, configID); err != nil {
		return err
	}
	if err := rt.initializeNodes(ctx, tx, qtx, runID, opts.StartNode, configPayload, nil); err != nil {
		return err
	}
	if opts.StartNode == "source_fetch" {
		if _, err := tx.Exec(ctx, `update workflow_node_runs set input_snapshot_id = $2 where run_id = $1 and node_key = 'source_fetch'`, runID, inputID); err != nil {
			return err
		}
	}
	if len(opts.SourceIDs) == 0 {
		return rt.completeEmpty(ctx, tx, runID, "no_enabled_sources")
	}
	if opts.StartNode != "source_fetch" {
		return rt.enqueueReplayStart(ctx, tx, qtx, runID, opts.StartNode, opts.ReplayScope)
	}
	for _, sourceID := range uniqueUUIDs(opts.SourceIDs) {
		if _, err := queue.Send(ctx, tx, queue.SourceFetch,
			map[string]any{"sourceId": sourceID, "workflowRunId": runID, "cascade": true},
			queue.WithCorrelation(runID), queue.WithTrigger(opts.TriggerType)); err != nil {
			return err
		}
	}
	return rt.appendEvent(ctx, qtx, runID, "source_fetch", "run_created", "queued", "工作流已创建，采集任务已入队", map[string]any{
		"sourceCount": len(uniqueUUIDs(opts.SourceIDs)),
	})
}

// CreateReplay creates an immutable child run and immediately queues the
// requested start node. Identical parent/node/scope/config requests are
// idempotent through replay_key.
func (rt *Runtime) CreateReplay(ctx context.Context, parentID uuid.UUID, scope map[string]any, fromNode string, reason *string, overrides map[string]any) (dbgen.WorkflowRun, error) {
	if err := validateNode(fromNode); err != nil {
		return dbgen.WorkflowRun{}, err
	}
	if err := validateReplayScope(scope); err != nil {
		return dbgen.WorkflowRun{}, err
	}
	if err := validateConfigOverrides(overrides); err != nil {
		return dbgen.WorkflowRun{}, err
	}
	parent, err := rt.q.GetWorkflowRun(ctx, parentID)
	if err != nil {
		return dbgen.WorkflowRun{}, err
	}
	if parent.Mode != workflowMode {
		return dbgen.WorkflowRun{}, fmt.Errorf("run %s is not a content workflow", parentID)
	}
	keyPayload := map[string]any{"parent": parentID, "from": fromNode, "scope": scope, "overrides": overrides}
	keyBytes, _ := json.Marshal(keyPayload)
	hash := sha256.Sum256(keyBytes)
	replayKey := hex.EncodeToString(hash[:])
	childID := uuid.New()
	replayScope := cloneMap(scope)
	replayScope["configOverrides"] = cloneMap(overrides)
	replayScope["replayFromNode"] = fromNode
	err = pgx.BeginFunc(ctx, rt.pool, func(tx pgx.Tx) error {
		qtx := dbgen.New(tx)
		var inserted uuid.UUID
		row := tx.QueryRow(ctx, `insert into workflow_runs
            (id, correlation_id, mode, start_node, status, metadata, trigger_type,
             parent_run_id, replay_from_node, replay_scope, input_snapshot_id, replay_key)
            values ($1, $1, $2, $3, 'queued', $4::jsonb, 'replay', $5, $3,
                    $6::jsonb, $7, $8)
            on conflict (replay_key) where replay_key is not null do nothing
            returning id`, childID, workflowMode, fromNode,
			jsonString(map[string]any{"parentRunId": parentID, "replayReason": deref(reason), "workflowVersion": workflowVersion}),
			parentID, jsonString(replayScope), nullableUUID(parent.InputSnapshotID), replayKey)
		if err := row.Scan(&inserted); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				if err := tx.QueryRow(ctx, `select id from workflow_runs where replay_key = $1`, replayKey).Scan(&inserted); err != nil {
					return err
				}
				childID = inserted
				return nil
			}
			return err
		}
		childID = inserted
		configPayload, _ := json.Marshal(map[string]any{
			"workflowVersion": workflowVersion, "triggerType": "replay", "overrides": cloneMap(overrides),
		})
		configID, err := rt.createSnapshot(ctx, qtx, inserted, "config", configPayload)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `update workflow_runs set config_snapshot_id = $2, updated_at = now() where id = $1`, inserted, configID); err != nil {
			return err
		}
		parentNode, err := rt.parentNodeForReplay(ctx, tx, parentID, fromNode)
		if err != nil {
			return err
		}
		if err := rt.initializeNodes(ctx, tx, qtx, inserted, fromNode, configPayload, parentNode); err != nil {
			return err
		}
		if replayMode(scope) == "evaluate_only" {
			if err := rt.skipReplayDownstream(ctx, tx, inserted, fromNode); err != nil {
				return err
			}
		}
		if fromNode == "source_fetch" && parent.InputSnapshotID.Valid {
			if _, err := tx.Exec(ctx, `update workflow_node_runs set input_snapshot_id = $2 where run_id = $1 and node_key = 'source_fetch'`, inserted, parent.InputSnapshotID.UUID); err != nil {
				return err
			}
		}
		if err := rt.appendEvent(ctx, qtx, inserted, fromNode, "run_created", "queued", "replay 工作流已创建", map[string]any{
			"parentRunId": parentID, "replayFromNode": fromNode, "replayScope": scope,
		}); err != nil {
			return err
		}
		return rt.enqueueReplayStart(ctx, tx, qtx, inserted, fromNode, scope)
	})
	if err != nil {
		return dbgen.WorkflowRun{}, err
	}
	return rt.q.GetWorkflowRun(ctx, childID)
}

func (rt *Runtime) updateRunMetadata(ctx context.Context, tx pgx.Tx, runID uuid.UUID, opts CreateOptions) error {
	_, err := tx.Exec(ctx, `update workflow_runs
        set trigger_type = $2, parent_run_id = $3, replay_from_node = $4,
            replay_scope = coalesce($5::jsonb, '{}'::jsonb), replay_key = $6,
            updated_at = now()
        where id = $1`, runID, opts.TriggerType, nullableUUIDValue(opts.ParentRunID), nullableText(opts.ReplayFromNode),
		nullableJSON(opts.ReplayScope), nullableTextValue(opts.ReplayKey))
	return err
}

func (rt *Runtime) initializeNodes(ctx context.Context, tx pgx.Tx, qtx *dbgen.Queries, runID uuid.UUID, startNode string, config []byte, parentNode *nodeRef) error {
	startIndex := nodeIndex(startNode)
	for index, node := range nodeOrder {
		if index < startIndex {
			if _, err := qtx.CreateWorkflowNodeRun(ctx, dbgen.CreateWorkflowNodeRunParams{RunID: runID, NodeKey: node, ConfigSnapshot: []byte(`{"skipped":"replay_before_start"}`)}); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `update workflow_node_runs set status = 'skipped', completed_at = now() where run_id = $1 and node_key = $2`, runID, node)
			if err != nil {
				return err
			}
			continue
		}
		if _, err := qtx.CreateWorkflowNodeRun(ctx, dbgen.CreateWorkflowNodeRunParams{RunID: runID, NodeKey: node, ConfigSnapshot: config}); err != nil {
			return err
		}
	}
	if parentNode != nil {
		_, err := tx.Exec(ctx, `update workflow_node_runs
            set input_snapshot_id = $2 where run_id = $1 and node_key = $3`, runID, nullableUUID(parentNode.outputSnapshotID), startNode)
		return err
	}
	return nil
}

type nodeRef struct {
	outputSnapshotID uuid.NullUUID
}

func (rt *Runtime) parentNodeForReplay(ctx context.Context, tx pgx.Tx, parentID uuid.UUID, fromNode string) (*nodeRef, error) {
	index := nodeIndex(fromNode)
	if index == 0 {
		return nil, nil
	}
	var output uuid.NullUUID
	err := tx.QueryRow(ctx, `select output_snapshot_id from workflow_node_runs where run_id = $1 and node_key = $2`, parentID, nodeOrder[index-1]).Scan(&output)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("parent run has no completed input for %s", fromNode)
	}
	if err != nil {
		return nil, err
	}
	if !output.Valid {
		return nil, fmt.Errorf("parent node %s has no output snapshot", nodeOrder[index-1])
	}
	return &nodeRef{outputSnapshotID: output}, nil
}

func (rt *Runtime) enqueueReplayStart(ctx context.Context, tx pgx.Tx, qtx *dbgen.Queries, runID uuid.UUID, startNode string, scope map[string]any) error {
	ids, err := rt.replayInputIDs(ctx, tx, runID, startNode, scope)
	if err != nil {
		return err
	}
	if len(ids) == 0 {
		return rt.completeEmpty(ctx, tx, runID, "replay_input_empty")
	}
	if startNode == "topic_scout" {
		payload := rt.replayPayload(ctx, tx, runID, map[string]any{"workflowRunId": runID, "rawItemIds": ids, "cascade": true})
		if _, err := queue.Send(ctx, tx, queue.TopicScout, payload, queue.WithCorrelation(runID), queue.WithTrigger("replay")); err != nil {
			return err
		}
		return rt.appendEvent(ctx, qtx, runID, startNode, "queued", "queued", "replay 节点任务已入队", map[string]any{"itemCount": len(ids)})
	}
	for _, id := range ids {
		var q queue.Name
		payload := rt.replayPayload(ctx, tx, runID, map[string]any{"workflowRunId": runID})
		switch startNode {
		case "source_fetch":
			q = queue.SourceFetch
			payload["sourceId"] = id
			payload["cascade"] = true
		case "topic_evaluate":
			q = queue.TopicEvaluate
			payload["topicId"] = id
		case "article_write":
			q = queue.ArticleWrite
			payload["topicId"] = id
			payload["platform"] = "xiaohongshu"
			payload["replay"] = true
		case "article_evaluate":
			q = queue.ArticleEvaluate
			payload["articleId"] = id
		case "human_review":
			return rt.setWaitingReview(ctx, tx, runID, ids)
		default:
			return fmt.Errorf("unsupported replay node %s", startNode)
		}
		if startNode == "article_write" {
			platforms, err := rt.topicPlatforms(ctx, tx, id)
			if err != nil {
				return err
			}
			for _, platform := range platforms {
				payload["platform"] = platform
				if _, err := queue.Send(ctx, tx, q, payload, queue.WithCorrelation(runID), queue.WithTrigger("replay")); err != nil {
					return err
				}
			}
			continue
		}
		if _, err := queue.Send(ctx, tx, q, payload, queue.WithCorrelation(runID), queue.WithTrigger("replay")); err != nil {
			return err
		}
	}
	return rt.appendEvent(ctx, qtx, runID, startNode, "queued", "queued", "replay 节点任务已入队", map[string]any{"itemCount": len(ids)})
}

func (rt *Runtime) replayInputIDs(ctx context.Context, tx pgx.Tx, runID uuid.UUID, startNode string, scope map[string]any) ([]uuid.UUID, error) {
	var parentID uuid.UUID
	err := tx.QueryRow(ctx, `select coalesce(parent_run_id, id) from workflow_runs where id = $1`, runID).Scan(&parentID)
	if err != nil {
		return nil, err
	}
	key := map[string]string{
		"source_fetch": "sourceIds", "topic_scout": "rawItemIds", "topic_evaluate": "topicIds",
		"article_write": "topicIds", "article_evaluate": "articleIds", "human_review": "articleIds",
	}[startNode]
	index := nodeIndex(startNode)
	snapshotID, err := rt.replaySourceSnapshot(ctx, tx, parentID, index)
	if err != nil {
		return nil, err
	}
	if !snapshotID.Valid {
		return nil, nil
	}
	var raw []byte
	if err := tx.QueryRow(ctx, `select payload from workflow_snapshots where id = $1`, snapshotID.UUID).Scan(&raw); err != nil {
		return nil, err
	}
	var body map[string]any
	if err := json.Unmarshal(raw, &body); err != nil {
		return nil, err
	}
	ids := parseUUIDs(body[key])
	if selected := parseUUIDs(scope["itemIds"]); len(selected) > 0 {
		ids = intersectUUIDs(ids, selected)
	}
	if replayMode(scope) == "failed_items" {
		failed, err := rt.failedDecisionItems(ctx, tx, parentID, startNode)
		if err != nil {
			return nil, err
		}
		ids = intersectUUIDs(ids, failed)
	}
	return ids, nil
}

// replaySourceSnapshot walks the selected parent and its ancestors, preferring
// the nearest run that actually produced the input needed by the replay node.
// Replay runs may skip upstream nodes, so using only the root run would lose
// the most recent generated article/topic outputs.
func (rt *Runtime) replaySourceSnapshot(ctx context.Context, tx pgx.Tx, parentID uuid.UUID, startIndex int) (uuid.NullUUID, error) {
	var snapshot uuid.NullUUID
	var err error
	if startIndex == 0 {
		err = tx.QueryRow(ctx, `
            with recursive ancestors as (
                select id, parent_run_id, 0 as depth from workflow_runs where id = $1
                union all
                select wr.id, wr.parent_run_id, a.depth + 1
                from workflow_runs wr join ancestors a on wr.id = a.parent_run_id
            )
            select wr.input_snapshot_id
            from ancestors a join workflow_runs wr on wr.id = a.id
            where wr.input_snapshot_id is not null
            order by a.depth asc limit 1`, parentID).Scan(&snapshot)
	} else {
		err = tx.QueryRow(ctx, `
            with recursive ancestors as (
                select id, parent_run_id, 0 as depth from workflow_runs where id = $1
                union all
                select wr.id, wr.parent_run_id, a.depth + 1
                from workflow_runs wr join ancestors a on wr.id = a.parent_run_id
            )
            select wnr.output_snapshot_id
            from ancestors a
            join workflow_node_runs wnr on wnr.run_id = a.id and wnr.node_key = $2
            where wnr.output_snapshot_id is not null
            order by a.depth asc limit 1`, parentID, nodeOrder[startIndex-1]).Scan(&snapshot)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.NullUUID{}, nil
	}
	return snapshot, err
}

func validateReplayScope(scope map[string]any) error {
	mode := replayMode(scope)
	switch mode {
	case "full", "failed_items", "selected_items", "evaluate_only":
	default:
		return fmt.Errorf("unsupported replay scope mode %q", mode)
	}
	if mode == "selected_items" && len(parseUUIDs(scope["itemIds"])) == 0 {
		return errors.New("selected_items replay requires non-empty itemIds")
	}
	return nil
}

func validateConfigOverrides(overrides map[string]any) error {
	if overrides == nil {
		return nil
	}
	allowed := map[string]struct{}{
		"agentVersion": {}, "promptVersion": {}, "rubricVersion": {},
		"topicRubricVersion": {}, "articleRubricVersion": {}, "weightVersion": {},
		"topicWeightVersion": {}, "articleWeightVersion": {}, "model": {},
		"topicScoutModel": {}, "topicJudgeModel": {}, "outlineModel": {},
		"draftModel": {}, "criticModel": {}, "articleJudgeModel": {},
		"passThreshold": {}, "topicPassThreshold": {}, "articlePassThreshold": {},
		"maxConcurrency": {}, "maxBatchSize": {},
	}
	for key, value := range overrides {
		if _, ok := allowed[key]; !ok {
			return fmt.Errorf("unsupported workflow config override %q", key)
		}
		switch key {
		case "passThreshold", "topicPassThreshold", "articlePassThreshold":
			if number, ok := numericOverride(value); !ok || number < 0 || number > 100 {
				return fmt.Errorf("workflow override %q must be between 0 and 100", key)
			}
		case "maxConcurrency":
			if number, ok := numericOverride(value); !ok || number < 1 || number > 32 || number != float64(int(number)) {
				return fmt.Errorf("workflow override %q must be an integer between 1 and 32", key)
			}
		case "maxBatchSize":
			if number, ok := numericOverride(value); !ok || number < 1 || number > 1000 || number != float64(int(number)) {
				return fmt.Errorf("workflow override %q must be an integer between 1 and 1000", key)
			}
		case "weightVersion", "topicWeightVersion", "articleWeightVersion":
			if number, ok := numericOverride(value); !ok || number < 1 || number != float64(int(number)) {
				return fmt.Errorf("workflow override %q must be a positive integer", key)
			}
		default:
			if text, ok := value.(string); !ok || strings.TrimSpace(text) == "" {
				return fmt.Errorf("workflow override %q must be a non-empty string", key)
			}
		}
	}
	return nil
}

func numericOverride(value any) (float64, bool) {
	switch typed := value.(type) {
	case float64:
		return typed, true
	case float32:
		return float64(typed), true
	case int:
		return float64(typed), true
	case int32:
		return float64(typed), true
	case int64:
		return float64(typed), true
	default:
		return 0, false
	}
}

func replayMode(scope map[string]any) string {
	if scope == nil {
		return "full"
	}
	mode := strings.ToLower(strings.TrimSpace(fmt.Sprint(scope["mode"])))
	if mode == "" {
		return "full"
	}
	return mode
}

func (rt *Runtime) skipReplayDownstream(ctx context.Context, tx pgx.Tx, runID uuid.UUID, fromNode string) error {
	start := nodeIndex(fromNode) + 1
	for _, node := range nodeOrder[start:] {
		if _, err := tx.Exec(ctx, `update workflow_node_runs
            set status = 'skipped', completed_at = now(), counts = $3::jsonb
            where run_id = $1 and node_key = $2`, runID, node, countsJSON(0, 0, 0, 1, 0, 0)); err != nil {
			return err
		}
	}
	return nil
}

func (rt *Runtime) replayPayload(ctx context.Context, tx pgx.Tx, runID uuid.UUID, payload map[string]any) map[string]any {
	var raw []byte
	if err := tx.QueryRow(ctx, `select replay_scope from workflow_runs where id = $1`, runID).Scan(&raw); err == nil {
		payload["workflowConfigOverrides"] = map[string]any{}
		var scope map[string]any
		if json.Unmarshal(raw, &scope) == nil {
			if overrides, ok := scope["configOverrides"].(map[string]any); ok {
				payload["workflowConfigOverrides"] = overrides
			}
		}
	}
	return payload
}

func (rt *Runtime) failedDecisionItems(ctx context.Context, tx pgx.Tx, parentID uuid.UUID, node string) ([]uuid.UUID, error) {
	itemType := "topic"
	if node == "article_evaluate" || node == "human_review" {
		itemType = "article"
	}
	rows, err := tx.Query(ctx, `select item_id from workflow_item_decisions where run_id = $1 and item_type = $2 and decision = 'failed'`, parentID, itemType)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// ReconcileAll advances every active content run. It is intentionally safe to
// call frequently; the run row lock and event checks make each transition
// idempotent across multiple Core instances.
func (rt *Runtime) ReconcileAll(ctx context.Context) error {
	rows, err := rt.pool.Query(ctx, `select id from workflow_runs
        where mode = 'content_production'
          and status in ('queued', 'running', 'waiting_human_review')
        order by created_at asc limit $1`, maxReconcileRuns)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var runID uuid.UUID
		if err := rows.Scan(&runID); err != nil {
			return err
		}
		if err := rt.ReconcileRun(ctx, runID); err != nil {
			rt.log.Warn("workflow reconcile failed", "run_id", runID, "error", err)
		}
	}
	return rows.Err()
}

func (rt *Runtime) ReconcileRun(ctx context.Context, runID uuid.UUID) error {
	return pgx.BeginFunc(ctx, rt.pool, func(tx pgx.Tx) error {
		var status, startNode string
		var metadata []byte
		if err := tx.QueryRow(ctx, `select status, start_node, metadata from workflow_runs where id = $1 for update`, runID).Scan(&status, &startNode, &metadata); err != nil {
			return err
		}
		if status != "queued" && status != "running" {
			return nil
		}
		if _, err := tx.Exec(ctx, `update workflow_runs set status = 'running', started_at = coalesce(started_at, now()), updated_at = now() where id = $1 and status = 'queued'`, runID); err != nil {
			return err
		}
		meta := jsonObject(metadata)
		startIndex := nodeIndex(startNode)
		for index := startIndex; index < len(nodeOrder); index++ {
			var done bool
			var err error
			switch nodeOrder[index] {
			case "source_fetch":
				done, err = rt.reconcileSourceFetch(ctx, tx, runID, meta)
			case "topic_scout":
				done, err = rt.reconcileTopicScout(ctx, tx, runID)
			case "topic_evaluate":
				done, err = rt.reconcileTopicEvaluate(ctx, tx, runID)
			case "article_write":
				done, err = rt.reconcileArticleWrite(ctx, tx, runID)
			case "article_evaluate":
				done, err = rt.reconcileArticleEvaluate(ctx, tx, runID)
			case "human_review":
				return rt.reconcileHumanReview(ctx, tx, runID)
			}
			if err != nil {
				return err
			}
			if !done {
				return nil
			}
		}
		return rt.reconcileHumanReview(ctx, tx, runID)
	})
}

func (rt *Runtime) reconcileSourceFetch(ctx context.Context, tx pgx.Tx, runID uuid.UUID, metadata map[string]any) (bool, error) {
	expected := intValue(metadata["sourceCount"])
	success, failed, err := rt.terminalEventCounts(ctx, tx, runID, "source_fetch", "sourceId")
	if err != nil {
		return false, err
	}
	if success+failed < expected {
		return false, nil
	}
	ids, err := rt.idsByCorrelation(ctx, tx, "raw_items", runID)
	if err != nil {
		return false, err
	}
	node, err := rt.node(ctx, tx, runID, "source_fetch")
	if err != nil {
		return false, err
	}
	outputID, err := rt.ensureOutputSnapshot(ctx, tx, runID, node, map[string]any{"rawItemIds": ids, "input": expected, "accepted": len(ids), "failed": failed})
	if err != nil {
		return false, err
	}
	status := "succeeded"
	if failed > 0 {
		status = "partial_failed"
	}
	if err := rt.updateNode(ctx, tx, node.id, status, outputID, countsJSON(expected, success, 0, 0, failed, len(ids))); err != nil {
		return false, err
	}
	if len(ids) == 0 {
		if expected > 0 && failed == expected {
			return true, rt.failRun(ctx, tx, runID, "all_source_fetch_failed")
		}
		return true, rt.skipAfter(ctx, tx, runID, "source_fetch", "no_new_raw_items")
	}
	if rt.replayStopsAfter(ctx, tx, runID, "source_fetch") {
		return true, rt.completeReplayOnly(ctx, tx, runID, "evaluate_only_source_fetch")
	}
	if err := rt.bindNextInput(ctx, tx, runID, "source_fetch", outputID); err != nil {
		return false, err
	}
	if err := rt.ensureQueuedScout(ctx, tx, runID, ids); err != nil {
		return false, err
	}
	return true, nil
}

func (rt *Runtime) reconcileTopicScout(ctx context.Context, tx pgx.Tx, runID uuid.UUID) (bool, error) {
	terminal, failed, err := rt.singleNodeTerminal(ctx, tx, runID, "topic_scout")
	if err != nil || !terminal {
		return false, err
	}
	ids, err := rt.idsByCorrelation(ctx, tx, "topics", runID)
	if err != nil {
		return false, err
	}
	node, err := rt.node(ctx, tx, runID, "topic_scout")
	if err != nil {
		return false, err
	}
	outputID, err := rt.ensureOutputSnapshot(ctx, tx, runID, node, map[string]any{"topicIds": ids, "input": len(ids), "accepted": len(ids), "failed": failed})
	if err != nil {
		return false, err
	}
	status := "succeeded"
	if failed > 0 {
		status = "failed"
	}
	if err := rt.updateNode(ctx, tx, node.id, status, outputID, countsJSON(len(ids), len(ids), 0, 0, failed, len(ids))); err != nil {
		return false, err
	}
	if len(ids) == 0 {
		if failed > 0 {
			return true, rt.failRun(ctx, tx, runID, "topic_scout_failed")
		}
		return true, rt.skipAfter(ctx, tx, runID, "topic_scout", "scout_produced_no_topics")
	}
	if rt.replayStopsAfter(ctx, tx, runID, "topic_scout") {
		return true, rt.completeReplayOnly(ctx, tx, runID, "evaluate_only_topic_scout")
	}
	if err := rt.bindNextInput(ctx, tx, runID, "topic_scout", outputID); err != nil {
		return false, err
	}
	if err := rt.ensureTopicEvaluateJobs(ctx, tx, runID, ids); err != nil {
		return false, err
	}
	return true, nil
}

func (rt *Runtime) reconcileTopicEvaluate(ctx context.Context, tx pgx.Tx, runID uuid.UUID) (bool, error) {
	ids, err := rt.idsFromNodeInput(ctx, tx, runID, "topic_evaluate", "topicIds")
	if err != nil {
		return false, err
	}
	if len(ids) == 0 {
		return true, rt.skipAfter(ctx, tx, runID, "topic_evaluate", "no_topics_to_evaluate")
	}
	terminal, failed, err := rt.itemsTerminal(ctx, tx, runID, "topic_evaluate", ids, "topicId", "topic_evaluations")
	if err != nil || !terminal {
		return false, err
	}
	accepted := make([]uuid.UUID, 0, len(ids))
	for _, id := range ids {
		var score pgtype.Numeric
		var evaluationID uuid.UUID
		err := tx.QueryRow(ctx, `select e.id, e.total_score from topic_evaluations e left join agent_runs ar on ar.id = e.agent_run_id where e.topic_id = $1 and ar.correlation_id = $2 order by e.created_at desc limit 1`, id, runID).Scan(&evaluationID, &score)
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			return false, err
		}
		value, err := numericFloat(score)
		if err != nil {
			return false, err
		}
		threshold := defaultTopicPass
		var recordedThreshold pgtype.Numeric
		if err := tx.QueryRow(ctx, `select threshold from workflow_item_decisions
            where run_id = $1 and node_run_id = (select id from workflow_node_runs where run_id = $1 and node_key = 'topic_evaluate')
              and item_id = $2 order by created_at desc limit 1`, runID, id).Scan(&recordedThreshold); err == nil && recordedThreshold.Valid {
			if configured, thresholdErr := numericFloat(recordedThreshold); thresholdErr == nil {
				threshold = configured
			}
		}
		if value >= threshold {
			if err := rt.transitionTopic(ctx, tx, id, "candidate", "scored", score, evaluationID, "topic score accepted"); err != nil && !errors.Is(err, pgx.ErrNoRows) {
				return false, err
			}
			if err := rt.transitionTopic(ctx, tx, id, "scored", "approved", score, evaluationID, "topic accepted by workflow gate"); err != nil && !errors.Is(err, pgx.ErrNoRows) {
				return false, err
			}
			// The writer agent only accepts topics that have entered the writing
			// state. Keep this transition in the workflow transaction so the
			// quality gate and its fan-out cannot be observed separately.
			if err := rt.transitionTopic(ctx, tx, id, "approved", "in_writing", score, evaluationID, "topic entered workflow writing stage"); err != nil && !errors.Is(err, pgx.ErrNoRows) {
				return false, err
			}
			accepted = append(accepted, id)
		} else {
			if err := rt.transitionTopic(ctx, tx, id, "candidate", "rejected", score, evaluationID, fmt.Sprintf("topic score %.2f below workflow threshold %.2f", value, threshold)); err != nil && !errors.Is(err, pgx.ErrNoRows) {
				return false, err
			}
		}
	}
	node, err := rt.node(ctx, tx, runID, "topic_evaluate")
	if err != nil {
		return false, err
	}
	outputID, err := rt.ensureOutputSnapshot(ctx, tx, runID, node, map[string]any{"topicIds": accepted, "input": len(ids), "accepted": len(accepted), "rejected": len(ids) - len(accepted), "failed": failed})
	if err != nil {
		return false, err
	}
	status := "succeeded"
	if failed > 0 {
		status = "partial_failed"
	}
	if err := rt.updateNode(ctx, tx, node.id, status, outputID, countsJSON(len(ids), len(accepted), len(ids)-len(accepted), 0, failed, len(accepted))); err != nil {
		return false, err
	}
	if len(accepted) == 0 {
		return true, rt.skipAfter(ctx, tx, runID, "topic_evaluate", "all_topics_rejected")
	}
	if rt.replayStopsAfter(ctx, tx, runID, "topic_evaluate") {
		return true, rt.completeReplayOnly(ctx, tx, runID, "evaluate_only_topic_evaluate")
	}
	if err := rt.bindNextInput(ctx, tx, runID, "topic_evaluate", outputID); err != nil {
		return false, err
	}
	return true, rt.ensureArticleWriteJobs(ctx, tx, runID, accepted)
}

func (rt *Runtime) reconcileArticleWrite(ctx context.Context, tx pgx.Tx, runID uuid.UUID) (bool, error) {
	topicIDs, err := rt.idsFromNodeInput(ctx, tx, runID, "article_write", "topicIds")
	if err != nil {
		return false, err
	}
	if len(topicIDs) == 0 {
		return true, rt.skipAfter(ctx, tx, runID, "article_write", "no_accepted_topics")
	}
	type target struct {
		topic, platform uuid.UUID
		platformName    string
	}
	targets, err := rt.articleTargets(ctx, tx, topicIDs)
	if err != nil {
		return false, err
	}
	if len(targets) == 0 {
		return true, rt.skipAfter(ctx, tx, runID, "article_write", "accepted_topics_have_no_platforms")
	}
	terminal, failed, err := rt.articleWriteTerminal(ctx, tx, runID, targets)
	if err != nil || !terminal {
		return false, err
	}
	articleIDs, err := rt.articlesForTopics(ctx, tx, runID, topicIDs)
	if err != nil {
		return false, err
	}
	node, err := rt.node(ctx, tx, runID, "article_write")
	if err != nil {
		return false, err
	}
	outputID, err := rt.ensureOutputSnapshot(ctx, tx, runID, node, map[string]any{"articleIds": articleIDs, "input": len(targets), "accepted": len(articleIDs), "failed": failed})
	if err != nil {
		return false, err
	}
	status := "succeeded"
	if failed > 0 {
		status = "partial_failed"
	}
	if err := rt.updateNode(ctx, tx, node.id, status, outputID, countsJSON(len(targets), len(articleIDs), 0, 0, failed, len(articleIDs))); err != nil {
		return false, err
	}
	if len(articleIDs) == 0 {
		return true, rt.skipAfter(ctx, tx, runID, "article_write", "all_writes_failed")
	}
	for _, topicID := range uniqueUUIDs(topicIDs) {
		if err := rt.transitionTopic(ctx, tx, topicID, "in_writing", "written", pgtype.Numeric{}, uuid.Nil, "workflow article writing completed"); err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return false, err
		}
	}
	if rt.replayStopsAfter(ctx, tx, runID, "article_write") {
		return true, rt.completeReplayOnly(ctx, tx, runID, "evaluate_only_article_write")
	}
	if err := rt.bindNextInput(ctx, tx, runID, "article_write", outputID); err != nil {
		return false, err
	}
	return true, rt.ensureArticleEvaluateJobs(ctx, tx, runID, articleIDs)
}

func (rt *Runtime) reconcileArticleEvaluate(ctx context.Context, tx pgx.Tx, runID uuid.UUID) (bool, error) {
	articleIDs, err := rt.idsFromNodeInput(ctx, tx, runID, "article_evaluate", "articleIds")
	if err != nil {
		return false, err
	}
	if len(articleIDs) == 0 {
		return true, rt.skipAfter(ctx, tx, runID, "article_evaluate", "no_articles_to_evaluate")
	}
	terminal, failed, err := rt.itemsTerminal(ctx, tx, runID, "article_evaluate", articleIDs, "articleId", "article_evaluations")
	if err != nil || !terminal {
		return false, err
	}
	accepted := make([]uuid.UUID, 0, len(articleIDs))
	for _, id := range articleIDs {
		var passed bool
		var score pgtype.Numeric
		var evaluationID uuid.UUID
		err := tx.QueryRow(ctx, `select e.id, e.passed, e.total_score from article_evaluations e left join agent_runs ar on ar.id = e.agent_run_id where e.article_id = $1 and ar.correlation_id = $2 order by e.created_at desc limit 1`, id, runID).Scan(&evaluationID, &passed, &score)
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			return false, err
		}
		var currentStatus dbgen.ArticleStatus
		if err := tx.QueryRow(ctx, `select status from articles where id = $1`, id).Scan(&currentStatus); err != nil {
			return false, err
		}
		// evaluate_only replay re-scores an existing review artifact. It must
		// not move the article backwards through the normal draft state machine.
		if currentStatus == dbgen.ArticleStatusDraft {
			if err := rt.transitionArticle(ctx, tx, id, "draft", "scored", score, evaluationID, "article evaluation completed"); err != nil && !errors.Is(err, pgx.ErrNoRows) {
				return false, err
			}
			if passed {
				if err := rt.transitionArticle(ctx, tx, id, "scored", "pending_review", score, evaluationID, "article passed quality gate"); err != nil && !errors.Is(err, pgx.ErrNoRows) {
					return false, err
				}
				accepted = append(accepted, id)
			}
		} else if currentStatus == dbgen.ArticleStatusPendingReview && passed {
			accepted = append(accepted, id)
		}
	}
	node, err := rt.node(ctx, tx, runID, "article_evaluate")
	if err != nil {
		return false, err
	}
	outputID, err := rt.ensureOutputSnapshot(ctx, tx, runID, node, map[string]any{"articleIds": accepted, "input": len(articleIDs), "accepted": len(accepted), "rejected": len(articleIDs) - len(accepted), "failed": failed})
	if err != nil {
		return false, err
	}
	status := "succeeded"
	if failed > 0 {
		status = "partial_failed"
	}
	if err := rt.updateNode(ctx, tx, node.id, status, outputID, countsJSON(len(articleIDs), len(accepted), len(articleIDs)-len(accepted), 0, failed, len(accepted))); err != nil {
		return false, err
	}
	if rt.replayStopsAfter(ctx, tx, runID, "article_evaluate") {
		return true, rt.completeReplayOnly(ctx, tx, runID, "evaluate_only_article_evaluate")
	}
	return true, rt.finishReviewStage(ctx, tx, runID, accepted, failed)
}

func (rt *Runtime) reconcileHumanReview(ctx context.Context, tx pgx.Tx, runID uuid.UUID) error {
	var count int
	if err := tx.QueryRow(ctx, `select count(*) from workflow_artifacts where run_id = $1 and node_key = 'human_review'`, runID).Scan(&count); err != nil {
		return err
	}
	if count > 0 {
		_, err := tx.Exec(ctx, `update workflow_runs set status = 'waiting_human_review', updated_at = now() where id = $1 and status in ('queued', 'running')`, runID)
		return err
	}
	return rt.completeEmpty(ctx, tx, runID, "no_article_passed_quality_gate")
}

func (rt *Runtime) ensureQueuedScout(ctx context.Context, tx pgx.Tx, runID uuid.UUID, rawIDs []uuid.UUID) error {
	var exists bool
	if err := tx.QueryRow(ctx, `select exists(select 1 from workflow_events where run_id = $1 and node_key = 'topic_scout' and event_type = 'queued')`, runID).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return nil
	}
	payload := map[string]any{"rawItemIds": rawIDs, "workflowRunId": runID, "cascade": true}
	if _, err := queue.Send(ctx, tx, queue.TopicScout, payload, queue.WithCorrelation(runID), queue.WithTrigger("worker")); err != nil {
		return err
	}
	return rt.appendEvent(ctx, dbgen.New(tx), runID, "topic_scout", "queued", "queued", "采集阶段完成，选题阶段已入队", map[string]any{"rawItemCount": len(rawIDs)})
}

func (rt *Runtime) ensureTopicEvaluateJobs(ctx context.Context, tx pgx.Tx, runID uuid.UUID, topicIDs []uuid.UUID) error {
	qtx := dbgen.New(tx)
	for _, topicID := range uniqueUUIDs(topicIDs) {
		var queued bool
		if err := tx.QueryRow(ctx, `select exists(select 1 from workflow_events where run_id = $1 and node_key = 'topic_evaluate' and payload->>'topicId' = $2 and event_type in ('queued', 'started', 'succeeded', 'failed'))`, runID, topicID.String()).Scan(&queued); err != nil {
			return err
		}
		if queued {
			continue
		}
		payload := rt.replayPayload(ctx, tx, runID, map[string]any{"topicId": topicID, "workflowRunId": runID})
		if _, err := queue.Send(ctx, tx, queue.TopicEvaluate, payload, queue.WithCorrelation(runID), queue.WithTrigger("worker")); err != nil {
			return err
		}
		if err := rt.appendEvent(ctx, qtx, runID, "topic_evaluate", "queued", "queued", "选题评估任务已入队", map[string]any{"topicId": topicID}); err != nil {
			return err
		}
	}
	return nil
}

func (rt *Runtime) ensureArticleWriteJobs(ctx context.Context, tx pgx.Tx, runID uuid.UUID, topicIDs []uuid.UUID) error {
	qtx := dbgen.New(tx)
	for _, topicID := range uniqueUUIDs(topicIDs) {
		platforms, err := rt.topicPlatforms(ctx, tx, topicID)
		if err != nil {
			return err
		}
		for _, platform := range platforms {
			var queued bool
			if err := tx.QueryRow(ctx, `select exists(select 1 from workflow_events where run_id = $1 and node_key = 'article_write' and payload->>'topicId' = $2 and payload->>'platform' = $3 and event_type in ('queued', 'started', 'succeeded', 'failed'))`, runID, topicID.String(), platform).Scan(&queued); err != nil {
				return err
			}
			if queued {
				continue
			}
			payload := rt.replayPayload(ctx, tx, runID, map[string]any{"topicId": topicID, "platform": platform, "workflowRunId": runID})
			if _, err := queue.Send(ctx, tx, queue.ArticleWrite, payload, queue.WithCorrelation(runID), queue.WithTrigger("worker")); err != nil {
				return err
			}
			if err := rt.appendEvent(ctx, qtx, runID, "article_write", "queued", "queued", "文章写作任务已入队", map[string]any{"topicId": topicID, "platform": platform}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (rt *Runtime) ensureArticleEvaluateJobs(ctx context.Context, tx pgx.Tx, runID uuid.UUID, articleIDs []uuid.UUID) error {
	qtx := dbgen.New(tx)
	for _, articleID := range uniqueUUIDs(articleIDs) {
		var evaluated bool
		if err := tx.QueryRow(ctx, `select exists(select 1 from workflow_events where run_id = $1 and node_key = 'article_evaluate' and payload->>'articleId' = $2 and event_type in ('queued', 'started', 'succeeded', 'failed'))`, runID, articleID.String()).Scan(&evaluated); err != nil {
			return err
		}
		if evaluated {
			continue
		}
		payload := rt.replayPayload(ctx, tx, runID, map[string]any{"articleId": articleID, "workflowRunId": runID})
		if _, err := queue.Send(ctx, tx, queue.ArticleEvaluate, payload, queue.WithCorrelation(runID), queue.WithTrigger("worker")); err != nil {
			return err
		}
		if err := rt.appendEvent(ctx, qtx, runID, "article_evaluate", "queued", "queued", "文章评估任务已入队", map[string]any{"articleId": articleID}); err != nil {
			return err
		}
	}
	return nil
}

func (rt *Runtime) finishReviewStage(ctx context.Context, tx pgx.Tx, runID uuid.UUID, articleIDs []uuid.UUID, failed int) error {
	qtx := dbgen.New(tx)
	for _, articleID := range uniqueUUIDs(articleIDs) {
		var title string
		if err := tx.QueryRow(ctx, `select title from articles where id = $1`, articleID).Scan(&title); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `insert into workflow_artifacts (run_id, node_key, artifact_type, artifact_id, title, metadata)
            values ($1, 'human_review', 'article', $2, $3, $4::jsonb)
            on conflict (run_id, node_key, artifact_type, artifact_id) do nothing`, runID, articleID, title, jsonString(map[string]any{"status": "pending_review"})); err != nil {
			return err
		}
	}
	status := "succeeded"
	if failed > 0 {
		status = "partial_failed"
	}
	node, err := rt.node(ctx, tx, runID, "human_review")
	if err != nil {
		return err
	}
	outputID, err := rt.ensureOutputSnapshot(ctx, tx, runID, node, map[string]any{"articleIds": articleIDs, "status": "pending_review", "failed": failed})
	if err != nil {
		return err
	}
	if err := rt.updateNode(ctx, tx, node.id, status, outputID, countsJSON(len(articleIDs), len(articleIDs), 0, 0, failed, len(articleIDs))); err != nil {
		return err
	}
	runStatus := "waiting_human_review"
	if failed > 0 {
		runStatus = "partial_failed"
	}
	if _, err := tx.Exec(ctx, `update workflow_runs set status = $2, summary = $3::jsonb, completed_at = null, updated_at = now() where id = $1`, runID, runStatus, jsonString(map[string]any{"finalArticles": articleIDs, "failed": failed})); err != nil {
		return err
	}
	return rt.appendEvent(ctx, qtx, runID, "human_review", "succeeded", status, "文章已进入人工审阅工作台", map[string]any{"articleCount": len(articleIDs)})
}

func (rt *Runtime) skipAfter(ctx context.Context, tx pgx.Tx, runID uuid.UUID, fromNode, reason string) error {
	start := nodeIndex(fromNode) + 1
	qtx := dbgen.New(tx)
	for _, node := range nodeOrder[start:] {
		if _, err := tx.Exec(ctx, `update workflow_node_runs set status = 'skipped', completed_at = now(), counts = $3::jsonb where run_id = $1 and node_key = $2`, runID, node, countsJSON(0, 0, 0, 1, 0, 0)); err != nil {
			return err
		}
		if err := rt.appendEvent(ctx, qtx, runID, node, "transitioned", "skipped", reason, map[string]any{"reasonCode": reason}); err != nil {
			return err
		}
	}
	return rt.completeEmpty(ctx, tx, runID, reason)
}

func (rt *Runtime) completeEmpty(ctx context.Context, tx pgx.Tx, runID uuid.UUID, reason string) error {
	_, err := tx.Exec(ctx, `update workflow_runs set status = 'completed_empty', completed_at = now(), updated_at = now(), summary = $2::jsonb where id = $1 and status not in ('completed', 'completed_empty', 'failed', 'cancelled')`, runID, jsonString(map[string]any{"reason": reason}))
	return err
}

func (rt *Runtime) failRun(ctx context.Context, tx pgx.Tx, runID uuid.UUID, reason string) error {
	_, err := tx.Exec(ctx, `update workflow_runs
        set status = 'failed', error_message = $2, completed_at = now(), updated_at = now(),
            summary = $3::jsonb
        where id = $1 and status not in ('completed', 'completed_empty', 'failed', 'cancelled')`,
		runID, reason, jsonString(map[string]any{"reason": reason}))
	return err
}

func (rt *Runtime) replayStopsAfter(ctx context.Context, tx pgx.Tx, runID uuid.UUID, node string) bool {
	var raw []byte
	if err := tx.QueryRow(ctx, `select replay_scope from workflow_runs where id = $1`, runID).Scan(&raw); err != nil {
		return false
	}
	var scope map[string]any
	if json.Unmarshal(raw, &scope) != nil {
		return false
	}
	return replayMode(scope) == "evaluate_only" && nodeIndex(node) == nodeIndex(scopeString(scope, "replayFromNode"))
}

func scopeString(scope map[string]any, key string) string {
	return strings.TrimSpace(fmt.Sprint(scope[key]))
}

func (rt *Runtime) completeReplayOnly(ctx context.Context, tx pgx.Tx, runID uuid.UUID, reason string) error {
	_, err := tx.Exec(ctx, `update workflow_runs set status = 'completed', completed_at = now(), updated_at = now(), summary = $2::jsonb where id = $1 and status in ('queued', 'running')`, runID, jsonString(map[string]any{"reason": reason, "evaluateOnly": true}))
	return err
}

func (rt *Runtime) node(ctx context.Context, tx pgx.Tx, runID uuid.UUID, key string) (nodeRow, error) {
	var out nodeRow
	err := tx.QueryRow(ctx, `select id, status, input_snapshot_id, output_snapshot_id from workflow_node_runs where run_id = $1 and node_key = $2`, runID, key).Scan(&out.id, &out.status, &out.inputSnapshotID, &out.outputSnapshotID)
	return out, err
}

type nodeRow struct {
	id               uuid.UUID
	status           string
	inputSnapshotID  uuid.NullUUID
	outputSnapshotID uuid.NullUUID
}

func (rt *Runtime) updateNode(ctx context.Context, tx pgx.Tx, id uuid.UUID, status string, output uuid.UUID, counts []byte) error {
	var outputValue any
	if output != uuid.Nil {
		outputValue = output
	}
	_, err := tx.Exec(ctx, `update workflow_node_runs set status = $2, output_snapshot_id = coalesce($3, output_snapshot_id), counts = $4::jsonb, completed_at = now() where id = $1`, id, status, outputValue, counts)
	return err
}

func (rt *Runtime) bindNextInput(ctx context.Context, tx pgx.Tx, runID uuid.UUID, node string, outputID uuid.UUID) error {
	index := nodeIndex(node)
	if index < 0 || index+1 >= len(nodeOrder) || outputID == uuid.Nil {
		return nil
	}
	_, err := tx.Exec(ctx, `update workflow_node_runs
        set input_snapshot_id = coalesce(input_snapshot_id, $3)
        where run_id = $1 and node_key = $2`, runID, nodeOrder[index+1], outputID)
	return err
}

func (rt *Runtime) ensureOutputSnapshot(ctx context.Context, tx pgx.Tx, runID uuid.UUID, node nodeRow, payload map[string]any) (uuid.UUID, error) {
	if node.outputSnapshotID.Valid {
		return node.outputSnapshotID.UUID, nil
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return uuid.Nil, err
	}
	return rt.createSnapshot(ctx, dbgen.New(tx), runID, "output", body)
}

func (rt *Runtime) createSnapshot(ctx context.Context, qtx *dbgen.Queries, runID uuid.UUID, kind string, payload []byte) (uuid.UUID, error) {
	hash := sha256.Sum256(canonicalJSON(payload))
	row, err := qtx.CreateWorkflowSnapshot(ctx, dbgen.CreateWorkflowSnapshotParams{RunID: runID, Kind: kind, Payload: payload, Sha256: hex.EncodeToString(hash[:])})
	return row.ID, err
}

// canonicalJSON matches PostgreSQL jsonb's normalized representation closely
// enough for stable checksums across write/read boundaries.
func canonicalJSON(payload []byte) []byte {
	var value any
	if err := json.Unmarshal(payload, &value); err != nil {
		return payload
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return payload
	}
	return canonical
}

func (rt *Runtime) appendEvent(ctx context.Context, qtx *dbgen.Queries, runID uuid.UUID, node, eventType, status, message string, payload map[string]any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = qtx.CreateWorkflowEvent(ctx, dbgen.CreateWorkflowEventParams{RunID: runID, NodeKey: node, EventType: eventType, Status: status, Message: message, Payload: body})
	return err
}

func (rt *Runtime) singleNodeTerminal(ctx context.Context, tx pgx.Tx, runID uuid.UUID, node string) (bool, int, error) {
	var success, failed int
	if err := tx.QueryRow(ctx, `select count(*) filter (where event_type = 'succeeded'), count(*) filter (where event_type = 'failed') from workflow_events where run_id = $1 and node_key = $2`, runID, node).Scan(&success, &failed); err != nil {
		return false, 0, err
	}
	return success+failed > 0, failed, nil
}

func (rt *Runtime) terminalEventCounts(ctx context.Context, tx pgx.Tx, runID uuid.UUID, node, identityKey string) (int, int, error) {
	var success, failed int
	query := fmt.Sprintf(`select count(distinct payload->>'%s') filter (where event_type = 'succeeded'), count(distinct payload->>'%s') filter (where event_type = 'failed') from workflow_events where run_id = $1 and node_key = $2`, identityKey, identityKey)
	if err := tx.QueryRow(ctx, query, runID, node).Scan(&success, &failed); err != nil {
		return 0, 0, err
	}
	return success, failed, nil
}

func (rt *Runtime) idsByCorrelation(ctx context.Context, tx pgx.Tx, table string, runID uuid.UUID) ([]uuid.UUID, error) {
	if table != "raw_items" && table != "topics" {
		return nil, fmt.Errorf("invalid workflow table %s", table)
	}
	rows, err := tx.Query(ctx, fmt.Sprintf(`select id from %s where correlation_id = $1 order by created_at asc`, table), runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (rt *Runtime) idsFromNodeInput(ctx context.Context, tx pgx.Tx, runID uuid.UUID, node, key string) ([]uuid.UUID, error) {
	var snapshot uuid.NullUUID
	if err := tx.QueryRow(ctx, `select input_snapshot_id from workflow_node_runs where run_id = $1 and node_key = $2`, runID, node).Scan(&snapshot); err != nil {
		return nil, err
	}
	if !snapshot.Valid {
		return nil, nil
	}
	var raw []byte
	if err := tx.QueryRow(ctx, `select payload from workflow_snapshots where id = $1`, snapshot.UUID).Scan(&raw); err != nil {
		return nil, err
	}
	var body map[string]any
	if err := json.Unmarshal(raw, &body); err != nil {
		return nil, err
	}
	return parseUUIDs(body[key]), nil
}

func (rt *Runtime) topicPlatforms(ctx context.Context, tx pgx.Tx, topicID uuid.UUID) ([]string, error) {
	var values []string
	err := tx.QueryRow(ctx, `select coalesce(array(select unnest(target_platforms)::text), '{}') from topics where id = $1`, topicID).Scan(&values)
	return values, err
}

func (rt *Runtime) articleTargets(ctx context.Context, tx pgx.Tx, topicIDs []uuid.UUID) ([]struct {
	topic, platform uuid.UUID
	platformName    string
}, error) {
	rows, err := tx.Query(ctx, `select id, unnest(target_platforms)::text from topics where id = any($1) order by id`, topicIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var targets []struct {
		topic, platform uuid.UUID
		platformName    string
	}
	for rows.Next() {
		var topic uuid.UUID
		var platform string
		if err := rows.Scan(&topic, &platform); err != nil {
			return nil, err
		}
		targets = append(targets, struct {
			topic, platform uuid.UUID
			platformName    string
		}{topic: topic, platformName: platform})
	}
	return targets, rows.Err()
}

func (rt *Runtime) articleWriteTerminal(ctx context.Context, tx pgx.Tx, runID uuid.UUID, targets []struct {
	topic, platform uuid.UUID
	platformName    string
}) (bool, int, error) {
	failed := 0
	for _, target := range targets {
		var found bool
		if err := tx.QueryRow(ctx, `select exists(select 1 from articles where topic_id = $1 and platform = $2::platform and correlation_id = $3)`, target.topic, target.platformName, runID).Scan(&found); err != nil {
			return false, 0, err
		}
		if found {
			continue
		}
		var failure bool
		if err := tx.QueryRow(ctx, `select exists(select 1 from workflow_events where run_id = $1 and node_key = 'article_write' and event_type = 'failed' and payload->>'topicId' = $2 and payload->>'platform' = $3)`, runID, target.topic.String(), target.platformName).Scan(&failure); err != nil {
			return false, 0, err
		}
		if failure {
			failed++
			continue
		}
		return false, failed, nil
	}
	return true, failed, nil
}

func (rt *Runtime) articlesForTopics(ctx context.Context, tx pgx.Tx, runID uuid.UUID, topicIDs []uuid.UUID) ([]uuid.UUID, error) {
	rows, err := tx.Query(ctx, `select distinct on (topic_id, platform) id from articles where topic_id = any($1) and correlation_id = $2 order by topic_id, platform, version desc, created_at desc`, topicIDs, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (rt *Runtime) itemsTerminal(ctx context.Context, tx pgx.Tx, runID uuid.UUID, node string, ids []uuid.UUID, identityKey, evaluationTable string) (bool, int, error) {
	failed := 0
	columnByTable := map[string]string{"topic_evaluations": "topic_id", "article_evaluations": "article_id"}
	column, ok := columnByTable[evaluationTable]
	if !ok {
		return false, 0, fmt.Errorf("invalid evaluation table %s", evaluationTable)
	}
	for _, id := range ids {
		var evaluated bool
		if err := tx.QueryRow(ctx, fmt.Sprintf(`select exists(select 1 from %s e left join agent_runs ar on ar.id = e.agent_run_id where e.%s = $1 and ar.correlation_id = $2)`, evaluationTable, column), id, runID).Scan(&evaluated); err != nil {
			return false, 0, err
		}
		if evaluated {
			continue
		}
		var itemFailure bool
		if err := tx.QueryRow(ctx, fmt.Sprintf(`select exists(select 1 from workflow_events where run_id = $1 and node_key = $2 and event_type = 'failed' and payload->>'%s' = $3)`, identityKey), runID, node, id.String()).Scan(&itemFailure); err != nil {
			return false, 0, err
		}
		if itemFailure {
			failed++
			continue
		}
		return false, failed, nil
	}
	return true, failed, nil
}

func (rt *Runtime) transitionTopic(ctx context.Context, tx pgx.Tx, id uuid.UUID, from, to string, score pgtype.Numeric, evaluationID uuid.UUID, reason string) error {
	_, err := dbgen.New(tx).TransitionTopic(ctx, dbgen.TransitionTopicParams{
		TopicID: id, FromStatus: dbgen.TopicStatus(from), ToStatus: dbgen.TopicStatus(to), Score: score,
		ActorType: "system", TriggerType: "harvester", TriggerID: pgtype.Text{String: evaluationID.String(), Valid: true},
		Reason: pgtype.Text{String: reason, Valid: true}, CorrelationID: uuid.NullUUID{UUID: id, Valid: false},
	})
	return err
}

func (rt *Runtime) transitionArticle(ctx context.Context, tx pgx.Tx, id uuid.UUID, from, to string, score pgtype.Numeric, evaluationID uuid.UUID, reason string) error {
	_, err := dbgen.New(tx).TransitionArticle(ctx, dbgen.TransitionArticleParams{
		ArticleID: id, FromStatus: dbgen.ArticleStatus(from), ToStatus: dbgen.ArticleStatus(to), Score: score,
		ActorType: "system", TriggerType: "harvester", TriggerID: pgtype.Text{String: evaluationID.String(), Valid: true},
		Reason: pgtype.Text{String: reason, Valid: true}, CorrelationID: uuid.NullUUID{UUID: id, Valid: false},
	})
	return err
}

func (rt *Runtime) setWaitingReview(ctx context.Context, tx pgx.Tx, runID uuid.UUID, articleIDs []uuid.UUID) error {
	for _, articleID := range uniqueUUIDs(articleIDs) {
		var title string
		if err := tx.QueryRow(ctx, `select title from articles where id = $1`, articleID).Scan(&title); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `insert into workflow_artifacts (run_id, node_key, artifact_type, artifact_id, title, metadata) values ($1, 'human_review', 'article', $2, $3, $4::jsonb) on conflict do nothing`, runID, articleID, title, jsonString(map[string]any{"status": "pending_review", "replay": true})); err != nil {
			return err
		}
	}
	node, err := rt.node(ctx, tx, runID, "human_review")
	if err != nil {
		return err
	}
	outputID, err := rt.ensureOutputSnapshot(ctx, tx, runID, node, map[string]any{"articleIds": uniqueUUIDs(articleIDs), "status": "pending_review", "replay": true})
	if err != nil {
		return err
	}
	if err := rt.updateNode(ctx, tx, node.id, "succeeded", outputID, countsJSON(len(articleIDs), len(articleIDs), 0, 0, 0, len(articleIDs))); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `update workflow_runs set status = 'waiting_human_review', updated_at = now(), summary = $2::jsonb where id = $1`, runID, jsonString(map[string]any{"finalArticles": articleIDs}))
	return err
}

func (rt *Runtime) numericScore(raw pgtype.Numeric) (float64, error) {
	value, err := raw.Float64Value()
	if err != nil || !value.Valid {
		if err == nil {
			err = errors.New("null numeric score")
		}
		return 0, err
	}
	return value.Float64, nil
}

func numericFloat(raw pgtype.Numeric) (float64, error) {
	value, err := raw.Float64Value()
	if err != nil || !value.Valid {
		if err == nil {
			err = errors.New("null numeric score")
		}
		return 0, err
	}
	return value.Float64, nil
}

func definitionPayload() []byte {
	body, _ := json.Marshal(map[string]any{"name": workflowMode, "version": workflowVersion, "nodes": nodeOrder})
	return body
}

func countsJSON(input, accepted, rejected, skipped, failed, output int) []byte {
	body, _ := json.Marshal(map[string]int{"input": input, "accepted": accepted, "rejected": rejected, "skipped": skipped, "failed": failed, "output": output})
	return body
}

func jsonObject(raw []byte) map[string]any {
	value := map[string]any{}
	_ = json.Unmarshal(raw, &value)
	return value
}

func jsonString(value any) string {
	body, _ := json.Marshal(value)
	return string(body)
}

func cloneMap(value map[string]any) map[string]any {
	if value == nil {
		return map[string]any{}
	}
	out := make(map[string]any, len(value))
	for key, item := range value {
		out[key] = item
	}
	return out
}

func parseUUIDs(value any) []uuid.UUID {
	var items []any
	switch typed := value.(type) {
	case []any:
		items = typed
	case []string:
		items = make([]any, len(typed))
		for index, item := range typed {
			items[index] = item
		}
	case []uuid.UUID:
		items = make([]any, len(typed))
		for index, item := range typed {
			items[index] = item
		}
	default:
		return nil
	}
	ids := make([]uuid.UUID, 0, len(items))
	for _, item := range items {
		if id, err := uuid.Parse(fmt.Sprint(item)); err == nil {
			ids = append(ids, id)
		}
	}
	return uniqueUUIDs(ids)
}

func uniqueUUIDs(ids []uuid.UUID) []uuid.UUID {
	seen := make(map[uuid.UUID]struct{}, len(ids))
	out := make([]uuid.UUID, 0, len(ids))
	for _, id := range ids {
		if id == uuid.Nil {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

func intersectUUIDs(left, right []uuid.UUID) []uuid.UUID {
	set := make(map[uuid.UUID]struct{}, len(right))
	for _, id := range right {
		set[id] = struct{}{}
	}
	out := make([]uuid.UUID, 0, len(left))
	for _, id := range left {
		if _, ok := set[id]; ok {
			out = append(out, id)
		}
	}
	return uniqueUUIDs(out)
}

func nodeIndex(node string) int {
	for index, value := range nodeOrder {
		if value == node {
			return index
		}
	}
	return -1
}

func validateNode(node string) error {
	if nodeIndex(node) < 0 {
		return fmt.Errorf("unknown workflow node %s", node)
	}
	return nil
}

func intValue(value any) int {
	switch typed := value.(type) {
	case int:
		return typed
	case float64:
		return int(typed)
	case json.Number:
		parsed, _ := typed.Int64()
		return int(parsed)
	default:
		return 0
	}
}

func deref(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func nullableUUID(value uuid.NullUUID) any {
	if value.Valid {
		return value.UUID
	}
	return nil
}

func nullableUUIDValue(value *uuid.UUID) any {
	if value == nil {
		return nil
	}
	return *value
}

func nullableText(value *string) any {
	if value == nil {
		return nil
	}
	return *value
}

func nullableTextValue(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func nullableJSON(value map[string]any) any {
	if value == nil {
		return nil
	}
	return jsonString(value)
}
