-- name: CreateWorkflowRun :one
insert into workflow_runs (id, correlation_id, mode, start_node, status, metadata)
values ($1, $2, $3, $4, 'queued', $5)
returning *;

-- name: GetWorkflowRun :one
select * from workflow_runs where id = $1;

-- name: ListWorkflowRuns :many
select * from workflow_runs order by created_at desc limit $1;

-- name: CreateWorkflowNodeRun :one
insert into workflow_node_runs (run_id, node_key, status, config_snapshot)
values ($1, $2, 'queued', $3)
on conflict (run_id, node_key) do update set config_snapshot = excluded.config_snapshot
returning *;

-- name: GetWorkflowNodeRun :one
select * from workflow_node_runs where id = $1;

-- name: ListWorkflowNodeRuns :many
select * from workflow_node_runs where run_id = $1 order by created_at asc;

-- name: UpdateWorkflowNodeRun :one
update workflow_node_runs
set status = $2,
    input_snapshot_id = coalesce($3, input_snapshot_id),
    output_snapshot_id = coalesce($4, output_snapshot_id),
    counts = coalesce($5, counts),
    started_at = case when $2 = 'running' then coalesce(started_at, now()) else started_at end,
    completed_at = case when $2 in ('succeeded', 'partial_failed', 'failed', 'skipped', 'cancelled') then now() else completed_at end
where id = $1
returning *;

-- name: CreateWorkflowSnapshot :one
insert into workflow_snapshots (run_id, kind, payload, sha256)
values ($1, $2, $3, $4)
returning *;

-- name: ListWorkflowDecisions :many
select * from workflow_item_decisions
where run_id = $1 and ($2 = '' or node_run_id = $2::uuid) and ($3 = '' or decision = $3)
order by created_at asc;

-- name: CreateWorkflowDecision :one
insert into workflow_item_decisions
    (run_id, node_run_id, item_id, item_type, decision, reason_code, reason,
     dimension_scores, total_score, threshold, weight_version, rubric_version,
     input_refs, evidence_refs, agent_run_id, trace_id)
values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
returning *;

-- name: StartWorkflowRun :one
update workflow_runs
set status = 'running', started_at = coalesce(started_at, now())
where id = $1 and status = 'queued'
returning *;

-- name: FinishWorkflowRun :one
update workflow_runs
set status = $2, error_message = $3, completed_at = now(), updated_at = now()
where id = $1 and status not in ('completed', 'completed_empty', 'partial_failed', 'failed', 'cancelled')
returning *;

-- name: CreateWorkflowEvent :one
insert into workflow_events (run_id, node_key, event_type, status, message, agent_run_id, payload)
values ($1, $2, $3, $4, $5, $6, $7)
returning *;

-- name: ListWorkflowEvents :many
select * from workflow_events
where run_id = $1 and sequence > $2
order by sequence asc
limit $3;

-- name: CreateWorkflowArtifact :one
insert into workflow_artifacts (run_id, node_key, artifact_type, artifact_id, title, metadata)
values ($1, $2, $3, $4, $5, $6)
on conflict (run_id, artifact_type, artifact_id) do update
set title = excluded.title, metadata = excluded.metadata
returning *;

-- name: ListWorkflowArtifacts :many
select * from workflow_artifacts
where run_id = $1
order by created_at asc;

-- name: ListEnabledSourceIDs :many
select id from sources where enabled = true and archived_at is null order by created_at asc;

-- name: WorkflowRunReadyToFinish :one
with latest_articles as (
    select distinct on (a.topic_id, a.platform) a.status
    from articles a
    join topics t on t.id = a.topic_id
    where t.correlation_id = $1
    order by a.topic_id, a.platform, a.version desc
)
select exists (select 1 from latest_articles)
   and not exists (
       select 1 from latest_articles
       where status not in ('pending_review', 'approved', 'published', 'rejected')
   );

-- name: MarkWorkflowRunSucceeded :one
update workflow_runs
set status = 'completed', completed_at = now(), updated_at = now()
where id = $1 and status in ('queued', 'running')
returning *;

-- name: WorkflowRunHasFailedEvent :one
select exists (
    select 1 from workflow_events
    where run_id = $1 and event_type = 'failed'
);

-- name: ListWorkflowRawItems :many
select id, title from raw_items where correlation_id = $1 order by created_at asc;

-- name: ListWorkflowTopics :many
select id, title from topics where correlation_id = $1 order by created_at asc;

-- name: ListWorkflowArticles :many
select a.id, a.title from articles a
join topics t on t.id = a.topic_id
where t.correlation_id = $1
order by a.created_at asc;
