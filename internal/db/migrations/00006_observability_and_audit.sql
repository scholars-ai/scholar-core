-- +goose Up
-- 跨服务 correlation、状态审计、失败留痕和评分理由。
-- 所有字段均为 nullable/default，保持历史数据和旧消息兼容。

alter table sources add column archived_at timestamptz;

alter table raw_items
    add column correlation_id uuid,
    add column ingest_note text;
create index raw_items_correlation_idx on raw_items (correlation_id)
    where correlation_id is not null;

alter table topics add column correlation_id uuid;
create index topics_correlation_idx on topics (correlation_id)
    where correlation_id is not null;

alter table topic_evaluations
    add column dimension_reasons jsonb not null default '{}';

alter table agent_runs add column correlation_id uuid;
create index agent_runs_correlation_idx on agent_runs (correlation_id)
    where correlation_id is not null;

create table state_transition_events (
    id            uuid primary key default gen_random_uuid(),
    entity_type   text not null,
    entity_id     uuid not null,
    from_status   text not null,
    to_status     text not null,
    actor_type    text not null check (actor_type in ('user', 'system', 'agent')),
    actor_id      text,
    trigger_type  text not null check (trigger_type in ('api', 'scheduler', 'harvester', 'worker')),
    trigger_id    text,
    reason        text,
    correlation_id uuid,
    metadata      jsonb not null default '{}',
    created_at    timestamptz not null default now()
);
create index state_transition_entity_idx
    on state_transition_events (entity_type, entity_id, created_at desc);
create index state_transition_correlation_idx
    on state_transition_events (correlation_id)
    where correlation_id is not null;

create table job_failures (
    id              uuid primary key default gen_random_uuid(),
    queue           text not null,
    msg_id          bigint not null,
    job_id          uuid,
    correlation_id  uuid,
    payload         jsonb not null,
    read_count      int not null check (read_count >= 1),
    error_type      text not null,
    error_message   text not null,
    retryable       boolean not null,
    archived        boolean not null default false,
    created_at      timestamptz not null default now(),
    unique (queue, msg_id)
);
create index job_failures_created_idx on job_failures (created_at desc);
create index job_failures_correlation_idx on job_failures (correlation_id)
    where correlation_id is not null;

-- 成功回执以 job_id 幂等；消息已完成但 Worker 在 delete 前崩溃时，重投只做删除。
create table job_receipts (
    job_id          uuid primary key,
    queue           text not null,
    msg_id          bigint not null,
    correlation_id  uuid,
    completed_at    timestamptz not null default now()
);
create index job_receipts_correlation_idx on job_receipts (correlation_id)
    where correlation_id is not null;

create table source_fetch_runs (
    id              uuid primary key default gen_random_uuid(),
    source_id       uuid not null references sources(id),
    job_id          uuid,
    correlation_id  uuid,
    attempt         int not null check (attempt >= 1),
    ok              boolean not null,
    stats           jsonb not null default '{}',
    error_type      text,
    error_message   text,
    started_at      timestamptz not null,
    finished_at     timestamptz not null,
    created_at      timestamptz not null default now()
);
create index source_fetch_runs_source_idx
    on source_fetch_runs (source_id, created_at desc);
create index source_fetch_runs_correlation_idx
    on source_fetch_runs (correlation_id)
    where correlation_id is not null;

-- +goose Down
drop table source_fetch_runs;
drop table job_receipts;
drop table job_failures;
drop table state_transition_events;
drop index agent_runs_correlation_idx;
alter table agent_runs drop column correlation_id;
alter table topic_evaluations drop column dimension_reasons;
drop index topics_correlation_idx;
alter table topics drop column correlation_id;
drop index raw_items_correlation_idx;
alter table raw_items drop column ingest_note, drop column correlation_id;
alter table sources drop column archived_at;
