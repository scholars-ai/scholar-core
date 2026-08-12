-- name: GetSchedulerSettings :one
select settings, updated_at from scheduler_settings where id = true;

-- name: UpsertSchedulerSettings :one
insert into scheduler_settings (id, settings) values (true, $1)
on conflict (id) do update set settings = excluded.settings, updated_at = now()
returning settings, updated_at;

-- name: SeedSchedulerSettings :exec
-- 仅在表为空时写入默认值（SPEC-008 §3.2：环境变量只 seed 首次，不覆盖用户设置）。
insert into scheduler_settings (id, settings) values (true, $1)
on conflict (id) do nothing;

-- name: RecordScheduleRun :one
-- 防重投：同一 schedule_key + planned_at 唯一，冲突返回 0 行 = 本窗口已投递过。
insert into schedule_runs (schedule_key, planned_at, queue, msg_id, note)
values ($1, $2, $3, $4, $5)
on conflict (schedule_key, planned_at) do nothing
returning id;

-- name: RecentScheduleRuns :many
select * from schedule_runs order by enqueued_at desc limit $1;

-- name: CountNewRawItems :one
-- topic_scout 的 min_new_items 闸门用
select count(*) from raw_items where status = 'new';

-- name: ActiveWeightSet :one
-- Judge 取当前生效权重：最大 version
select version, weights from weight_sets
where rubric_id = $1 order by version desc limit 1;
