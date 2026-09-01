-- +goose Up
-- SPEC-010: immutable run snapshots, node runs, item decisions and replay metadata.
alter table workflow_runs drop constraint if exists workflow_runs_status_check;
update workflow_runs set status = 'completed' where status = 'succeeded';
update workflow_runs set mode = 'content_production' where mode = 'cascade';
alter table workflow_runs add constraint workflow_runs_status_check check (
    status in ('queued', 'running', 'waiting_human_review', 'completed', 'completed_empty', 'partial_failed', 'failed', 'cancelled')
);
alter table workflow_runs drop constraint if exists workflow_runs_mode_check;
alter table workflow_runs add constraint workflow_runs_mode_check check (mode in ('cascade', 'content_production'));
alter table workflow_runs add column if not exists trigger_type text not null default 'manual';
alter table workflow_runs add constraint workflow_runs_trigger_type_check check (trigger_type in ('scheduled', 'manual', 'replay'));
alter table workflow_runs add column if not exists parent_run_id uuid references workflow_runs(id);
alter table workflow_runs add column if not exists replay_from_node text;
alter table workflow_runs add column if not exists replay_scope jsonb not null default '{}';
alter table workflow_runs add column if not exists input_snapshot_id uuid;
alter table workflow_runs add column if not exists config_snapshot_id uuid;
alter table workflow_runs add column if not exists summary jsonb not null default '{}';
alter table workflow_runs add column if not exists updated_at timestamptz not null default now();
create index if not exists workflow_runs_parent_idx on workflow_runs(parent_run_id, created_at desc);

create table if not exists workflow_snapshots (
    id uuid primary key default gen_random_uuid(),
    run_id uuid not null references workflow_runs(id),
    kind text not null check (kind in ('definition', 'input', 'output', 'config')),
    payload jsonb not null default '{}',
    sha256 text not null check (length(sha256) = 64),
    created_at timestamptz not null default now()
);
create index if not exists workflow_snapshots_run_idx on workflow_snapshots(run_id, kind, created_at);

create table if not exists workflow_node_runs (
    id uuid primary key default gen_random_uuid(),
    run_id uuid not null references workflow_runs(id),
    node_key text not null check (node_key in ('source_fetch', 'topic_scout', 'topic_evaluate', 'article_write', 'article_evaluate', 'human_review')),
    status text not null default 'queued' check (status in ('queued', 'running', 'succeeded', 'partial_failed', 'failed', 'skipped', 'cancelled')),
    input_snapshot_id uuid references workflow_snapshots(id),
    output_snapshot_id uuid references workflow_snapshots(id),
    config_snapshot jsonb not null default '{}',
    counts jsonb not null default '{}',
    created_at timestamptz not null default now(),
    started_at timestamptz,
    completed_at timestamptz,
    unique (run_id, node_key)
);
create index if not exists workflow_node_runs_run_idx on workflow_node_runs(run_id, created_at);

create table if not exists workflow_item_decisions (
    id uuid primary key default gen_random_uuid(),
    run_id uuid not null references workflow_runs(id),
    node_run_id uuid not null references workflow_node_runs(id),
    item_id uuid not null,
    item_type text not null check (item_type in ('raw_item', 'topic', 'article')),
    decision text not null check (decision in ('accepted', 'rejected', 'skipped', 'failed')),
    reason_code text not null,
    reason text not null default '',
    dimension_scores jsonb,
    total_score numeric(5,2),
    threshold numeric(5,2),
    weight_version int,
    rubric_version text,
    input_refs jsonb not null default '{}',
    evidence_refs jsonb not null default '{}',
    agent_run_id uuid,
    trace_id text,
    created_at timestamptz not null default now()
);
create index if not exists workflow_decisions_node_idx on workflow_item_decisions(run_id, node_run_id, decision, created_at);
create index if not exists workflow_decisions_item_idx on workflow_item_decisions(item_type, item_id, created_at);

alter table workflow_artifacts add column if not exists parent_artifact_id uuid references workflow_artifacts(id);
alter table workflow_artifacts add column if not exists snapshot_id uuid references workflow_snapshots(id);

-- +goose Down
alter table workflow_artifacts drop column if exists snapshot_id;
alter table workflow_artifacts drop column if exists parent_artifact_id;
drop table if exists workflow_item_decisions;
drop table if exists workflow_node_runs;
drop table if exists workflow_snapshots;
alter table workflow_runs drop column if exists updated_at;
alter table workflow_runs drop column if exists summary;
alter table workflow_runs drop column if exists config_snapshot_id;
alter table workflow_runs drop column if exists input_snapshot_id;
alter table workflow_runs drop column if exists replay_scope;
alter table workflow_runs drop column if exists replay_from_node;
alter table workflow_runs drop column if exists parent_run_id;
alter table workflow_runs drop constraint if exists workflow_runs_trigger_type_check;
alter table workflow_runs drop column if exists trigger_type;
