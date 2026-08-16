-- name: ListSources :many
select s.*,
       h.last_run_at, h.last_success_at, h.next_run_at,
       coalesce(h.consecutive_failures, 0) as consecutive_failures,
       h.last_error,
       (select count(*) from raw_items r where r.source_id = s.id) as item_count
from sources s
left join source_health h on h.source_id = s.id
where s.enabled = coalesce(sqlc.narg('enabled')::boolean, s.enabled)
  and s.archived_at is null
order by s.name;

-- name: GetSource :one
select s.*,
       h.last_run_at, h.last_success_at, h.next_run_at,
       coalesce(h.consecutive_failures, 0) as consecutive_failures,
       h.last_error,
       (select count(*) from raw_items r where r.source_id = s.id) as item_count
from sources s
left join source_health h on h.source_id = s.id
where s.id = $1 and s.archived_at is null;

-- name: CreateSource :one
insert into sources (name, type, url, category, weight, enabled, fetch_config)
values ($1, $2, $3, $4, $5, $6, $7)
returning *;

-- name: UpdateSource :one
update sources set
    name = coalesce(sqlc.narg('name'), name),
    url = case when sqlc.arg('set_url')::boolean then sqlc.narg('url') else url end,
    category = coalesce(sqlc.narg('category')::source_category, category),
    weight = coalesce(sqlc.narg('weight'), weight),
    enabled = coalesce(sqlc.narg('enabled'), enabled),
    fetch_config = coalesce(sqlc.narg('fetch_config'), fetch_config),
    updated_at = now()
where id = $1
returning *;

-- name: DeleteSource :execrows
-- 历史素材必须保留其 source 外键和名称，因此 DELETE API 实现为归档。
update sources
set enabled = false, archived_at = now(), updated_at = now()
where id = $1 and archived_at is null;

-- name: DueSources :many
-- 调度器 tick 用：到期该采集的 enabled 源。
-- next_run_at 为空（新源/从未跑过）视为立即到期。
select s.id, s.name, s.fetch_config
from sources s
left join source_health h on h.source_id = s.id
where s.enabled
  and s.archived_at is null
  and s.type <> 'manual'
  and (h.next_run_at is null or h.next_run_at <= now());

-- name: MarkSourceScheduled :exec
-- 投递后立刻写 next_run_at，防同一窗口重复投递（与入队同事务）。
insert into source_health (source_id, last_run_at, next_run_at)
values ($1, now(), $2)
on conflict (source_id) do update set
    last_run_at = now(), next_run_at = excluded.next_run_at, updated_at = now();

-- name: RecordSourceResult :exec
-- agents 采集完成后由 core 收割写入（成功清零连败，失败累加）。
insert into source_health (source_id, last_success_at, consecutive_failures, last_error)
values (
    $1,
    case when sqlc.arg('ok')::boolean then now() else null end,
    case when sqlc.arg('ok')::boolean then 0 else 1 end,
    sqlc.narg('error')
)
on conflict (source_id) do update set
    last_success_at = case when sqlc.arg('ok')::boolean then now() else source_health.last_success_at end,
    consecutive_failures = case when sqlc.arg('ok')::boolean then 0 else source_health.consecutive_failures + 1 end,
    last_error = sqlc.narg('error'),
    updated_at = now();
