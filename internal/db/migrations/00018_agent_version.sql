-- +goose Up
-- SPEC-010: persist the implementation version that actually executed an Agent job.
alter table agent_runs add column if not exists agent_version text;
create index if not exists agent_runs_agent_version_idx
    on agent_runs(agent_version, created_at desc)
    where agent_version is not null;

-- +goose Down
drop index if exists agent_runs_agent_version_idx;
alter table agent_runs drop column if exists agent_version;
