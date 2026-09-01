-- +goose Up
-- SPEC-010: reversible snapshot lifecycle metadata for retention/archive.
alter table workflow_snapshots add column if not exists archived_at timestamptz;
alter table workflow_snapshots add column if not exists storage_ref text;
alter table workflow_snapshots add column if not exists retention_until timestamptz;
create index if not exists workflow_snapshots_retention_idx
    on workflow_snapshots(retention_until)
    where archived_at is null and retention_until is not null;

-- +goose Down
drop index if exists workflow_snapshots_retention_idx;
alter table workflow_snapshots drop column if exists retention_until;
alter table workflow_snapshots drop column if exists storage_ref;
alter table workflow_snapshots drop column if exists archived_at;
