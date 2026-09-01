-- +goose Up
-- SPEC-010: replay idempotency and one immutable decision per item/node.
alter table workflow_runs add column if not exists replay_key text;
create unique index if not exists workflow_runs_replay_key_idx
    on workflow_runs(replay_key) where replay_key is not null;

-- Older development databases may contain duplicate decisions from worker
-- retries. Keep the newest observation before enforcing one decision per item.
with ranked as (
    select id,
           row_number() over (
               partition by run_id, node_run_id, item_id
               order by created_at desc, id desc
           ) as position
    from workflow_item_decisions
)
delete from workflow_item_decisions d
using ranked r
where d.id = r.id and r.position > 1;

create unique index if not exists workflow_item_decisions_item_once_idx
    on workflow_item_decisions(run_id, node_run_id, item_id);

-- +goose Down
drop index if exists workflow_item_decisions_item_once_idx;
drop index if exists workflow_runs_replay_key_idx;
alter table workflow_runs drop column if exists replay_key;
