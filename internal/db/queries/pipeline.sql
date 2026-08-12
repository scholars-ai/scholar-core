-- name: PendingCandidates :many
-- harvester：等待投递评分的 candidate（事件驱动，SPEC-008 §3.1 纪律 2）。
-- 用 schedule_runs 的唯一约束防重投，此处只取状态。
select id, title from topics where status = 'candidate' order by created_at limit $1;

-- name: LatestEvaluation :one
-- harvester：收割某选题最近一次评分
select id, topic_id, total_score, created_at from topic_evaluations
where topic_id = $1 order by created_at desc limit 1;

-- name: ScoredTopicsWithoutTransition :many
-- harvester：已有评分但状态仍是 candidate 的选题（评分完成，待推进状态机）
select t.id, t.status, e.total_score, e.id as evaluation_id
from topics t
join lateral (
    select id, total_score from topic_evaluations
    where topic_id = t.id order by created_at desc limit 1
) e on true
where t.status = 'candidate'
limit $1;

-- name: TransitionTopic :one
-- 状态机唯一写入口：带前置状态条件的 CAS 更新。0 行 = 前置状态不符（并发或非法流转）。
update topics set status = sqlc.arg('to_status'), latest_score = coalesce(sqlc.narg('score'), latest_score), updated_at = now()
where id = $1 and status = sqlc.arg('from_status')
returning *;

-- name: ListTopicEvaluations :many
select * from topic_evaluations where topic_id = $1 order by created_at desc;

-- name: CreateManualRawItem :one
-- 手动投喂：先落一条占位 raw_item（内容抓取由 agents 的 source_fetch 完成后更新）
insert into raw_items (source_id, title, url, content, content_hash, status)
values ($1, $2, $3, $4, $5, 'new')
on conflict (content_hash) do nothing
returning id;

-- name: GetManualSource :one
-- 手动投喂共用的内置 manual 源（seed 于迁移或首次使用时创建）
select id from sources where type = 'manual' and name = 'Manual Feed' limit 1;

-- name: CreateManualSource :one
insert into sources (name, type, url, category, weight, enabled, fetch_config)
values ('Manual Feed', 'manual', null, 'news', 0.8, true, '{"role": "material", "full_text": "fetch_page"}')
on conflict (name) do update set updated_at = now()
returning id;
