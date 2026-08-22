-- +goose Up
-- 固定 DAG 的运行记录、不可变事件和产物索引。
create table workflow_runs (
    id              uuid primary key,
    correlation_id  uuid not null unique,
    mode            text not null default 'cascade' check (mode in ('cascade')),
    start_node      text not null default 'source_fetch',
    status          text not null default 'queued' check (status in ('queued', 'running', 'succeeded', 'failed')),
    error_message   text,
    metadata        jsonb not null default '{}',
    created_at      timestamptz not null default now(),
    started_at      timestamptz,
    completed_at    timestamptz
);
create index workflow_runs_created_idx on workflow_runs (created_at desc);
create index workflow_runs_status_idx on workflow_runs (status, created_at desc);

create table workflow_events (
    id              uuid primary key default gen_random_uuid(),
    run_id          uuid not null references workflow_runs(id) on delete cascade,
    sequence        bigint generated always as identity,
    node_key        text not null,
    event_type      text not null check (event_type in ('run_created', 'queued', 'started', 'progress', 'artifact_created', 'transitioned', 'succeeded', 'failed', 'retrying')),
    status          text not null check (status in ('queued', 'running', 'succeeded', 'failed', 'skipped')),
    message         text not null default '',
    agent_run_id    uuid,
    payload         jsonb not null default '{}',
    occurred_at     timestamptz not null default now(),
    unique (run_id, sequence)
);
create index workflow_events_run_sequence_idx on workflow_events (run_id, sequence);

create table workflow_artifacts (
    id              uuid primary key default gen_random_uuid(),
    run_id          uuid not null references workflow_runs(id) on delete cascade,
    node_key        text not null,
    artifact_type   text not null check (artifact_type in ('raw_item', 'topic', 'evaluation', 'article')),
    artifact_id     uuid not null,
    title           text not null default '',
    metadata        jsonb not null default '{}',
    created_at      timestamptz not null default now(),
    unique (run_id, artifact_type, artifact_id)
);
create index workflow_artifacts_run_idx on workflow_artifacts (run_id, created_at);

-- +goose Down
drop table workflow_artifacts;
drop table workflow_events;
drop table workflow_runs;
