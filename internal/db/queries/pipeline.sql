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

-- M2.1：固定节奏自动选择最高分 Topic，Core 随后执行 scored → approved。
-- 只返回仍处于 scored 的 Topic，避免重复自动批准。
-- name: ScoredTopicsForScheduledWriting :many
select id, correlation_id
from topics
where status = 'scored'
order by latest_score desc nulls last, updated_at desc, id
limit $1;

-- name: GetPipelineCounts :one
select
    (select count(*) from raw_items) as raw_total,
    (select count(*) from raw_items where status = 'new') as raw_new,
    (select count(*) from raw_items where status = 'clustered') as raw_clustered,
    (select count(*) from raw_items where status = 'discarded') as raw_discarded,
    (select count(*) from topics) as topic_total,
    (select count(*) from topics where status = 'scored') as topic_scored,
    (select count(*) from topics where status in ('approved', 'in_writing', 'written')) as topic_passed,
    (select count(*) from topics where status = 'rejected') as topic_rejected,
    (select count(*) from articles) as article_total,
    (select count(*) from articles where status in ('pending_review', 'approved')) as article_ready,
    (select count(*) from articles where status in ('approved', 'published')) as article_passed,
    (select count(*) from articles where status = 'rejected') as article_rejected,
    (select count(*) from articles where status = 'rewrite_queued' or version > 1) as article_rewrites;

-- name: LastPipelineScheduleRuns :many
select distinct on (stage_key)
    id, schedule_key, planned_at, enqueued_at, queue, msg_id, note, stage_key
from (
    select schedule_runs.*,
           (case
               when schedule_key like 'source_fetch:%' then 'source_fetch'
               when schedule_key = 'topic_scout' then 'topic_scout'
               when schedule_key = 'article_write_batch' then 'article_write'
           end)::text as stage_key
    from schedule_runs
    where schedule_key like 'source_fetch:%'
       or schedule_key in ('topic_scout', 'article_write_batch')
) runs
where stage_key is not null
order by stage_key, planned_at desc;

-- name: RecentPipelineFailures :many
select id, queue, error_type, error_message, retryable, created_at
from job_failures
where archived = false
order by created_at desc
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

-- name: ArticleEvaluationsWithoutTransition :many
-- ArticleJudge 已写回评分，但 Article 仍是 draft；先推进到 scored。
select a.id, a.topic_id, a.platform, a.version, t.correlation_id,
       e.id as evaluation_id, e.total_score
from articles a
join topics t on t.id = a.topic_id
join lateral (
    select id, total_score
    from article_evaluations
    where article_id = a.id
    order by created_at desc
    limit 1
) e on true
where a.status = 'draft'
order by a.updated_at, a.id
limit $1;

-- name: ScoredArticlesWithoutDecision :many
-- scored 是显式审计节点；随后按确定性 passed 判定进入人工终审或回炉。
select a.id, a.topic_id, a.platform, a.version, t.correlation_id,
       e.id as evaluation_id, e.total_score, e.passed, e.pass_threshold,
       e.rationale, e.dimension_scores, e.dimension_reasons, e.vetoed_dimension
from articles a
join topics t on t.id = a.topic_id
join lateral (
    select id, total_score, passed, pass_threshold, rationale,
           dimension_scores, dimension_reasons, vetoed_dimension
    from article_evaluations
    where article_id = a.id
    order by created_at desc
    limit 1
) e on true
where a.status = 'scored'
order by a.updated_at, a.id
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

-- name: TransitionArticle :one
-- Article 状态机唯一写入口：CAS 更新与审计事件同一 SQL 完成。
with transitioned as (
    update articles
    set status = sqlc.arg('to_status'),
        latest_score = coalesce(sqlc.narg('score'), latest_score),
        updated_at = now()
    where articles.id = sqlc.arg('article_id') and articles.status = sqlc.arg('from_status')
    returning articles.*
), audited as (
    insert into state_transition_events (
        entity_type, entity_id, from_status, to_status,
        actor_type, actor_id, trigger_type, trigger_id, reason,
        correlation_id, metadata
    )
    select 'article', transitioned.id, sqlc.arg('from_status')::text, sqlc.arg('to_status')::text,
           sqlc.arg('actor_type')::text, sqlc.narg('actor_id')::text,
           sqlc.arg('trigger_type')::text, sqlc.narg('trigger_id')::text,
           sqlc.narg('reason')::text, sqlc.narg('correlation_id')::uuid,
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
