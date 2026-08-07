-- +goose Up
-- SPEC-002 数据模型全量初始化。实体结构与 scholar-shared/schemas/scholars.schema.json 对齐。

-- 扩展：pgvector（语义检索）、pgmq（任务队列，ADR-003）
create extension if not exists vector;
create extension if not exists pgmq;

-- 枚举（与 shared 的 JSON Schema enum 双向对齐，SPEC-002 §4）
create type platform as enum ('xiaohongshu', 'zhihu', 'wechat');
create type article_format as enum ('markdown');
create type source_type as enum ('rss', 'rsshub', 'manual', 'crawler');
create type source_category as enum ('news', 'research', 'tutorial', 'kol');
create type raw_item_status as enum ('new', 'clustered', 'discarded');
create type topic_status as enum ('candidate', 'scored', 'approved', 'in_writing', 'written', 'rejected');
create type article_status as enum ('draft', 'scored', 'rewrite_queued', 'pending_review', 'approved', 'published', 'rejected');
create type metric_source as enum ('manual', 'import', 'api');
create type insight_kind as enum ('topic_lesson', 'writing_lesson', 'platform_lesson', 'source_lesson');
create type insight_status as enum ('candidate', 'active', 'retired');
create type agent_run_status as enum ('running', 'succeeded', 'failed');

create table sources (
    id            uuid primary key default gen_random_uuid(),
    name          text not null,
    type          source_type not null,
    url           text,
    category      source_category not null,
    weight        numeric(3,2) not null default 0.50 check (weight >= 0 and weight <= 1),
    enabled       boolean not null default true,
    fetch_config  jsonb not null default '{}',
    created_at    timestamptz not null default now(),
    updated_at    timestamptz not null default now()
);

create table raw_items (
    id            uuid primary key default gen_random_uuid(),
    source_id     uuid not null references sources(id),
    title         text not null,
    url           text,
    author        text,
    content       text not null,
    published_at  timestamptz,
    content_hash  text not null unique,
    embedding     vector(1024),
    status        raw_item_status not null default 'new',
    created_at    timestamptz not null default now(),
    updated_at    timestamptz not null default now()
);
create index raw_items_status_idx on raw_items (status);
create index raw_items_embedding_idx on raw_items using hnsw (embedding vector_cosine_ops);

create table topics (
    id                uuid primary key default gen_random_uuid(),
    title             text not null,
    angle             text not null default '',
    summary           text not null default '',
    raw_item_ids      uuid[] not null default '{}',
    target_platforms  platform[] not null check (cardinality(target_platforms) > 0),
    status            topic_status not null default 'candidate',
    latest_score      numeric(5,2) check (latest_score >= 0 and latest_score <= 100),
    embedding         vector(1024),
    created_at        timestamptz not null default now(),
    updated_at        timestamptz not null default now()
);
create index topics_status_score_idx on topics (status, latest_score desc nulls last);
create index topics_embedding_idx on topics using hnsw (embedding vector_cosine_ops);

create table topic_evaluations (
    id                uuid primary key default gen_random_uuid(),
    topic_id          uuid not null references topics(id),
    rubric_version    text not null,
    dimension_scores  jsonb not null,
    total_score       numeric(5,2) not null check (total_score >= 0 and total_score <= 100),
    rationale         text not null,
    judge_model       text not null default '',
    agent_run_id      uuid,
    created_at        timestamptz not null default now()
);
create index topic_evaluations_topic_idx on topic_evaluations (topic_id, created_at desc);

create table articles (
    id            uuid primary key default gen_random_uuid(),
    topic_id      uuid not null references topics(id),
    platform      platform not null,
    version       int not null default 1 check (version >= 1),
    format        article_format not null default 'markdown',
    title         text not null,
    content_md    text not null,
    assets        jsonb not null default '[]',
    writer_agent  text not null default '',
    status        article_status not null default 'draft',
    latest_score  numeric(5,2) check (latest_score >= 0 and latest_score <= 100),
    created_at    timestamptz not null default now(),
    updated_at    timestamptz not null default now(),
    unique (topic_id, platform, version)
);
create index articles_status_idx on articles (status);

create table article_evaluations (
    id                uuid primary key default gen_random_uuid(),
    article_id        uuid not null references articles(id),
    rubric_version    text not null,
    dimension_scores  jsonb not null,
    total_score       numeric(5,2) not null check (total_score >= 0 and total_score <= 100),
    rationale         text not null,
    judge_model       text not null default '',
    agent_run_id      uuid,
    created_at        timestamptz not null default now()
);
create index article_evaluations_article_idx on article_evaluations (article_id, created_at desc);

create table publications (
    id                         uuid primary key default gen_random_uuid(),
    article_id                 uuid not null references articles(id),
    platform                   platform not null,
    platform_post_id           text,
    published_at               timestamptz not null,
    final_content_diff         text,
    follower_count_at_publish  int check (follower_count_at_publish >= 0),
    created_at                 timestamptz not null default now()
);
create index publications_article_idx on publications (article_id);

create table metric_snapshots (
    id              uuid primary key default gen_random_uuid(),
    publication_id  uuid not null references publications(id),
    captured_at     timestamptz not null,
    metrics         jsonb not null,
    source          metric_source not null default 'manual',
    created_at      timestamptz not null default now(),
    unique (publication_id, captured_at)
);

create table insights (
    id          uuid primary key default gen_random_uuid(),
    kind        insight_kind not null,
    platform    platform,
    content     text not null,
    evidence    jsonb not null default '[]',
    confidence  numeric(3,2) not null check (confidence >= 0 and confidence <= 1),
    status      insight_status not null default 'candidate',
    embedding   vector(1024),
    created_at  timestamptz not null default now(),
    updated_at  timestamptz not null default now()
);
create index insights_kind_status_idx on insights (kind, status);
create index insights_embedding_idx on insights using hnsw (embedding vector_cosine_ops);

create table agent_runs (
    id                 uuid primary key default gen_random_uuid(),
    job_type           text not null,
    entity_type        text,
    entity_id          uuid,
    langfuse_trace_id  text,
    model              text,
    prompt_version     text,
    tokens_in          bigint check (tokens_in >= 0),
    tokens_out         bigint check (tokens_out >= 0),
    cost_usd           numeric(10,4) check (cost_usd >= 0),
    status             agent_run_status not null default 'running',
    created_at         timestamptz not null default now(),
    updated_at         timestamptz not null default now()
);
create index agent_runs_job_type_idx on agent_runs (job_type, created_at desc);

-- 校准后的生效权重（SPEC-004 §4），初始版本由 rubric YAML 的 initialWeight 灌入
create table weight_sets (
    id          uuid primary key default gen_random_uuid(),
    rubric_id   text not null,
    version     int not null check (version >= 1),
    weights     jsonb not null,
    note        text not null default '',
    created_at  timestamptz not null default now(),
    unique (rubric_id, version)
);

-- pgmq 队列（名称与 scholar-shared/schemas/queues.json 一致，[a-z_] 限定）
select pgmq.create('source_fetch');
select pgmq.create('topic_scout');
select pgmq.create('topic_evaluate');
select pgmq.create('article_write');
select pgmq.create('article_evaluate');
select pgmq.create('memory_reflect');

-- +goose Down
select pgmq.drop_queue('memory_reflect');
select pgmq.drop_queue('article_evaluate');
select pgmq.drop_queue('article_write');
select pgmq.drop_queue('topic_evaluate');
select pgmq.drop_queue('topic_scout');
select pgmq.drop_queue('source_fetch');
drop table weight_sets;
drop table agent_runs;
drop table insights;
drop table metric_snapshots;
drop table publications;
drop table article_evaluations;
drop table articles;
drop table topic_evaluations;
drop table topics;
drop table raw_items;
drop table sources;
drop type agent_run_status;
drop type insight_status;
drop type insight_kind;
drop type metric_source;
drop type article_status;
drop type topic_status;
drop type raw_item_status;
drop type source_category;
drop type source_type;
drop type article_format;
drop type platform;
