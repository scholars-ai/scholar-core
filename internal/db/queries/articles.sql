-- name: ListArticles :many
select a.*, t.title as topic_title,
       (select count(*) from publications p where p.article_id = a.id) as publication_count
from articles a
join topics t on t.id = a.topic_id
where a.status = coalesce(sqlc.narg('status')::article_status, a.status)
  and a.platform = coalesce(sqlc.narg('platform')::platform, a.platform)
  and a.topic_id = coalesce(sqlc.narg('topic_id')::uuid, a.topic_id)
order by a.updated_at desc, a.id
limit sqlc.arg('lim') offset sqlc.arg('off');

-- name: CountArticles :one
select count(*)
from articles a
where a.status = coalesce(sqlc.narg('status')::article_status, a.status)
  and a.platform = coalesce(sqlc.narg('platform')::platform, a.platform)
  and a.topic_id = coalesce(sqlc.narg('topic_id')::uuid, a.topic_id);

-- name: GetArticle :one
select * from articles where id = $1;

-- name: GetArticleForUpdate :one
select * from articles where id = $1 for update;

-- name: ListArticleVersions :many
select * from articles
where topic_id = $1 and platform = $2
order by version desc, created_at desc;

-- name: LatestArticleEvaluation :one
select id, article_id, rubric_version, dimension_scores, dimension_reasons,
       total_score, rationale, judge_model, agent_run_id, weight_version,
       vetoed_dimension, pass_threshold, passed, created_at
from article_evaluations
where article_id = $1
order by created_at desc
limit 1;

-- name: ListArticleEvaluations :many
select id, article_id, rubric_version, dimension_scores, dimension_reasons,
       total_score, rationale, judge_model, agent_run_id, weight_version,
       vetoed_dimension, pass_threshold, passed, created_at
from article_evaluations
where article_id = $1
order by created_at desc;

-- name: ListArticlePublications :many
select * from publications
where article_id = $1
order by published_at desc, created_at desc;

-- name: CreatePublication :one
insert into publications (
    article_id, platform, platform_post_id, published_at,
    final_content_diff, edit_ratio, follower_count_at_publish
) values (
    sqlc.arg('article_id'), sqlc.arg('platform'), sqlc.arg('platform_post_id'),
    sqlc.arg('published_at'), sqlc.arg('final_content_diff'),
    sqlc.arg('edit_ratio'), sqlc.narg('follower_count_at_publish')
)
returning *;
