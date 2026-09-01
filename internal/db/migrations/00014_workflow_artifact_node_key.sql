-- +goose Up
-- A single article can be represented by multiple workflow stages (for
-- example article_write and human_review). Keep each stage's artifact index.
alter table workflow_artifacts
    drop constraint if exists workflow_artifacts_run_id_artifact_type_artifact_id_key;
create unique index if not exists workflow_artifacts_stage_artifact_idx
    on workflow_artifacts(run_id, node_key, artifact_type, artifact_id);

-- +goose Down
drop index if exists workflow_artifacts_stage_artifact_idx;
alter table workflow_artifacts
    add constraint workflow_artifacts_run_id_artifact_type_artifact_id_key
    unique (run_id, artifact_type, artifact_id);
