-- +goose Up
-- M3：标准指标窗口、可回放表现分、每周反思报告与人工记忆治理。

create type metric_window as enum ('h24', 'h72', 'd7', 'custom');

alter table metric_snapshots
    add column snapshot_window metric_window not null default 'custom',
    add column performance_raw numeric(18,4) not null default 0 check (performance_raw >= 0),
    add column performance_percentile numeric(7,4)
        check (performance_percentile is null or (performance_percentile >= 0 and performance_percentile <= 100)),
    add column performance_weight_version int not null default 1 check (performance_weight_version >= 1);

create unique index metric_snapshots_standard_window_unique
    on metric_snapshots (publication_id, snapshot_window)
    where snapshot_window <> 'custom';
create index metric_snapshots_performance_idx
    on metric_snapshots (snapshot_window, performance_percentile desc nulls last, captured_at desc);

create table performance_weight_sets (
    platform  platform not null,
    version   int not null check (version >= 1),
    weights   jsonb not null,
    note      text not null default '',
    created_at timestamptz not null default now(),
    primary key (platform, version)
);

insert into performance_weight_sets (platform, version, weights, note) values
('xiaohongshu', 1, '{"views":0.10,"likes":0.15,"favorites":0.35,"comments":0.10,"shares":0.05,"follows":0.25}', 'scholar-shared performance-weights.v1'),
('zhihu',       1, '{"views":0.10,"likes":0.35,"favorites":0.30,"comments":0.15,"shares":0.10,"follows":0.00}', 'scholar-shared performance-weights.v1'),
('wechat',      1, '{"views":0.30,"likes":0.15,"favorites":0.10,"comments":0.10,"shares":0.35,"follows":0.00}', 'scholar-shared performance-weights.v1');

alter table insights
    add column manual_status_override boolean not null default false;

create table weekly_reports (
    id                uuid primary key default gen_random_uuid(),
    period_start      timestamptz not null,
    period_end        timestamptz not null,
    sample_count      int not null default 0 check (sample_count >= 0),
    summary_markdown  text not null,
    calibration       jsonb not null default '{}',
    agent_run_id      uuid references agent_runs(id),
    created_at        timestamptz not null default now(),
    check (period_end > period_start),
    unique (period_start, period_end)
);
create index weekly_reports_period_idx on weekly_reports (period_end desc);

-- +goose Down
drop table weekly_reports;
alter table insights drop column manual_status_override;
drop table performance_weight_sets;
drop index metric_snapshots_performance_idx;
drop index metric_snapshots_standard_window_unique;
alter table metric_snapshots
    drop column performance_weight_version,
    drop column performance_percentile,
    drop column performance_raw,
    drop column snapshot_window;
drop type metric_window;
