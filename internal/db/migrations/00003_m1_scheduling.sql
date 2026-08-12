-- +goose Up
-- M1：调度配置入 DB（SPEC-008 §3.1）+ 信源采集健康状态 + 调度留痕 + topic@v1 权重 seed。

-- 全局调度设置：单行表（id 恒为 true）。结构由 shared 的 SchedulerSettings schema 约束，
-- API 层校验后整体写入 jsonb——避免每加一个配置项就改表结构。
-- DEFAULT_* 环境变量只在本表为空时 seed；用户改过后运行时真相只在这里（SPEC-008 §3.2）。
create table scheduler_settings (
    id          boolean primary key default true check (id),
    settings    jsonb not null,
    updated_at  timestamptz not null default now()
);

-- 信源采集健康状态（client 信源管理页展示；连续失败告警）。
-- 单独一张表而不是加列到 sources：健康状态是 agents/core 高频写的运行时数据，
-- 与人工编辑的订阅配置分开，互不干扰 updated_at 语义。
create table source_health (
    source_id             uuid primary key references sources(id) on delete cascade,
    last_run_at           timestamptz,
    last_success_at       timestamptz,
    next_run_at           timestamptz,
    consecutive_failures  int not null default 0 check (consecutive_failures >= 0),
    last_error            text,
    updated_at            timestamptz not null default now()
);

-- 调度留痕（SPEC-008 §3.1：每次调度记录 schedule key、触发时间、job id）。
-- 同时是防重投的依据：同一 schedule key + 同一计划时刻 唯一。
create table schedule_runs (
    id            uuid primary key default gen_random_uuid(),
    schedule_key  text not null,             -- source_fetch:<source_id> / topic_scout / manual:...
    planned_at    timestamptz not null,      -- 计划触发时刻（防重投锚点）
    enqueued_at   timestamptz not null default now(),
    queue         text not null,
    msg_id        bigint,
    note          text,                      -- 跳过原因等（如 min_new_items 不足）
    unique (schedule_key, planned_at)
);
create index schedule_runs_key_idx on schedule_runs (schedule_key, planned_at desc);

-- topic@v1 生效权重首版：与 rubrics/topic.v1.yaml 的 initialWeight 一致。
-- 之后由校准环节人工确认后插入新版本（SPEC-004 §4），Judge 取 max(version)。
insert into weight_sets (rubric_id, version, weights, note) values (
    'topic', 1,
    '{"timeliness": 0.20, "audience_value": 0.25, "platform_fit": 0.15,
      "differentiation": 0.15, "material_richness": 0.10, "history_signal": 0.15}',
    'seed from rubrics/topic.v1.yaml initialWeight (M1)'
);

-- +goose Down
delete from weight_sets where rubric_id = 'topic' and version = 1;
drop table schedule_runs;
drop table source_health;
drop table scheduler_settings;
