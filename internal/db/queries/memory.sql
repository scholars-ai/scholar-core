-- name: GetPublicationForMetrics :one
select p.id, p.article_id, p.platform, p.platform_post_id, p.published_at,
       p.final_content_diff, p.follower_count_at_publish, p.created_at, p.edit_ratio,
       a.title as article_title, t.title as topic_title
from publications p
join articles a on a.id = p.article_id
join topics t on t.id = a.topic_id
where p.id = $1;

-- name: ListPublicationsForMetrics :many
select p.id, p.article_id, p.platform, p.platform_post_id, p.published_at,
       p.final_content_diff, p.follower_count_at_publish, p.created_at, p.edit_ratio,
       a.title as article_title, t.title as topic_title
from publications p
join articles a on a.id = p.article_id
join topics t on t.id = a.topic_id
where p.platform = coalesce(sqlc.narg('platform')::platform, p.platform)
order by p.published_at desc
limit sqlc.arg('lim') offset sqlc.arg('off');

-- name: CountPublicationsForMetrics :one
select count(*) from publications p
where p.platform = coalesce(sqlc.narg('platform')::platform, p.platform);

-- name: ListMetricSnapshots :many
select id, publication_id, captured_at, metrics, source, created_at,
       snapshot_window, performance_raw, performance_percentile, performance_weight_version
from metric_snapshots
where publication_id = $1
order by captured_at asc, created_at asc;

-- name: ActivePerformanceWeights :one
select version, weights from performance_weight_sets
where platform = $1
order by version desc limit 1;

-- name: CreateMetricSnapshot :one
insert into metric_snapshots (
    publication_id, captured_at, metrics, source, snapshot_window,
    performance_raw, performance_percentile, performance_weight_version
) values ($1, $2, $3, $4, $5, $6, null, $7)
returning id, publication_id, captured_at, metrics, source, created_at,
          snapshot_window, performance_raw, performance_percentile, performance_weight_version;

-- name: RecomputePerformancePercentiles :exec
with ranked as (
    select ms.id,
           percent_rank() over (order by ms.performance_raw) * 100 as percentile
    from metric_snapshots ms
    join publications p on p.id = ms.publication_id
    where p.platform = sqlc.arg('platform')::platform
      and ms.snapshot_window = sqlc.arg('snapshot_window')::metric_window
      and ms.snapshot_window <> 'custom'
      and ms.captured_at >= now() - interval '90 days'
)
update metric_snapshots ms
set performance_percentile = ranked.percentile
from ranked
where ms.id = ranked.id;

-- name: ListInsights :many
select id, kind, platform, content, evidence, confidence, status, embedding,
       created_at, updated_at, manual_status_override
from insights
where (sqlc.narg('kind')::insight_kind is null or kind = sqlc.narg('kind')::insight_kind)
  and (sqlc.narg('status')::insight_status is null or status = sqlc.narg('status')::insight_status)
  and (
    sqlc.narg('platform')::platform is null
    or platform is null
    or platform = sqlc.narg('platform')::platform
  )
order by status = 'active' desc, confidence desc, updated_at desc
limit sqlc.arg('lim');

-- name: GetInsight :one
select id, kind, platform, content, evidence, confidence, status, embedding,
       created_at, updated_at, manual_status_override
from insights where id = $1;

-- name: UpdateInsightStatus :one
update insights
set status = $2, manual_status_override = true, updated_at = now()
where id = $1
returning id, kind, platform, content, evidence, confidence, status, embedding,
          created_at, updated_at, manual_status_override;

-- name: ListWeeklyReports :many
select id, period_start, period_end, sample_count, summary_markdown,
       calibration, agent_run_id, created_at
from weekly_reports
order by period_end desc
limit $1;
