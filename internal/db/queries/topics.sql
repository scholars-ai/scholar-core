-- name: ListTopics :many
select * from topics
where status = coalesce(sqlc.narg('status')::topic_status, status)
order by latest_score desc nulls last, created_at desc
limit sqlc.arg('lim') offset sqlc.arg('off');

-- name: CountTopics :one
select count(*) from topics
where status = coalesce(sqlc.narg('status')::topic_status, status);

-- name: GetTopic :one
select * from topics where id = $1;

-- name: CreateManualTopic :one
insert into topics (title, angle, summary, target_platforms, status)
values ($1, $2, $3, $4, 'candidate')
returning *;
