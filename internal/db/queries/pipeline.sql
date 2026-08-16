-- name: PendingCandidates :many
-- harvester：等待投递评分的 candidate（事件驱动，SPEC-008 §3.1 纪律 2）。
-- 用 schedule_runs 的唯一约束防重投，此处只取状态。
select id, title, correlation_id
from topics where status = 'candidate' order by created_at limit $1;

-- name: LatestEvaluation :one
-- harvester：收割某选题最近一次评分
select id, topic_id, total_score, created_at from topic_evaluations
where topic_id = $1 order by created_at desc limit 1;

-- name: ScoredTopicsWithoutTransition :many
-- harvester：已有评分但状态仍是 candidate 的选题（评分完成，待推进状态机）
select t.id, t.status, t.correlation_id, e.total_score, e.id as evaluation_id
from topics t
join lateral (
    select id, total_score from topic_evaluations
    where topic_id = t.id order by created_at desc limit 1
) e on true
where t.status = 'candidate'
limit $1;

-- name: PendingApprovedTopics :many
-- M2 harvester：等待按目标平台分派写作的 approved 选题。
select id, title, target_platforms, correlation_id
from topics
where status = 'approved'
order by updated_at, id
limit $1;

-- name: DraftArticlesPendingEvaluation :many
-- M2 harvester：Article 已由 agents 写回，但尚未投递独立评分任务。
-- schedule_runs 负责最终防重；这里保持查询简单，便于故障后重新扫描。
select a.id, a.topic_id, a.platform, t.correlation_id
from articles a
join topics t on t.id = a.topic_id
where a.status = 'draft'
order by a.created_at, a.id
limit $1;

-- name: InWritingTopicsReady :many
-- 只有 target_platforms 中每个平台都至少已有一篇文章，Topic 才算写作完成。
select t.id, t.correlation_id
from topics t
where t.status = 'in_writing'
  and not exists (
      select 1
      from unnest(t.target_platforms) as target(platform)
      where not exists (
          select 1
          from articles a
          where a.topic_id = t.id and a.platform = target.platform
      )
  )
order by t.updated_at, t.id
limit $1;

-- name: TransitionTopic :one
-- 状态机唯一写入口：CAS 更新、审计事件同一 SQL/事务完成。
with transitioned as (
    update topics
    set status = sqlc.arg('to_status'),
        latest_score = coalesce(sqlc.narg('score'), latest_score),
        updated_at = now()
    where topics.id = sqlc.arg('topic_id') and topics.status = sqlc.arg('from_status')
    returning topics.*
), audited as (
    insert into state_transition_events (
        entity_type, entity_id, from_status, to_status,
        actor_type, actor_id, trigger_type, trigger_id, reason,
        correlation_id, metadata
    )
    select 'topic', transitioned.id, sqlc.arg('from_status')::text, sqlc.arg('to_status')::text,
           sqlc.arg('actor_type')::text, sqlc.narg('actor_id')::text,
           sqlc.arg('trigger_type')::text, sqlc.narg('trigger_id')::text,
           sqlc.narg('reason')::text,
           coalesce(sqlc.narg('correlation_id')::uuid, transitioned.correlation_id),
           coalesce(sqlc.narg('metadata')::jsonb, '{}'::jsonb)
    from transitioned
)
select * from transitioned;

-- name: ListTopicEvaluations :many
select id, topic_id, rubric_version, dimension_scores, dimension_reasons, total_score, rationale,
       judge_model, agent_run_id, weight_version, vetoed_dimension, created_at
from topic_evaluations
where topic_id = $1
order by created_at desc;

-- name: CreateManualRawItem :one
-- 手动投喂：先落一条占位 raw_item（内容抓取由 agents 的 source_fetch 完成后更新）
insert into raw_items (source_id, title, url, content, content_hash, status)
values ($1, $2, $3, $4, $5, 'new')
on conflict (content_hash) do nothing
returning id;

-- name: GetManualSource :one
-- 手动投喂共用的内置 manual 源（seed 于迁移或首次使用时创建）
select id from sources
where type = 'manual' and name = 'Manual Feed' and archived_at is null
limit 1;

-- name: CreateManualSource :one
insert into sources (name, type, url, category, weight, enabled, fetch_config)
values ('Manual Feed', 'manual', null, 'news', 0.8, true, '{"role": "material", "full_text": "fetch_page"}')
on conflict (name) do update set
    enabled = true, archived_at = null, updated_at = now()
returning id;
