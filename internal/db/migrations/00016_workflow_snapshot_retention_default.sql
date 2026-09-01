-- +goose Up
-- SPEC-010: give existing and newly-created snapshots a visible retention deadline.
update workflow_snapshots
set retention_until = created_at + interval '168 hours'
where retention_until is null;
alter table workflow_snapshots
    alter column retention_until set default (now() + interval '168 hours');

-- +goose Down
alter table workflow_snapshots
    alter column retention_until drop default;
