-- +goose Up
-- M2：文章版本不可变。回炉生成新行，通过 previous_article_id 形成线性版本链。
alter table articles
    add column previous_article_id uuid references articles(id),
    add constraint articles_previous_not_self check (previous_article_id is null or previous_article_id <> id);
create unique index articles_previous_unique
    on articles (previous_article_id) where previous_article_id is not null;

-- 文章评分与 Topic 评分保持同等可回放信息，并固化本次过审判定。
alter table article_evaluations
    add column dimension_reasons jsonb not null default '{}',
    add column weight_version int,
    add column vetoed_dimension text,
    add column pass_threshold numeric(5,2) not null default 70
        check (pass_threshold >= 0 and pass_threshold <= 100),
    add column passed boolean not null default false;

insert into weight_sets (rubric_id, version, weights, note) values
(
    'article/xiaohongshu', 1,
    '{"accuracy":0.25,"structure":0.15,"readability":0.15,"format_compliance":0.10,"title_appeal":0.12,"conversational_voice":0.10,"scanability":0.08,"utility":0.05}',
    'seed from article-xiaohongshu.v1.yaml (M2)'
),
(
    'article/zhihu', 1,
    '{"accuracy":0.25,"structure":0.15,"readability":0.15,"format_compliance":0.10,"argument_depth":0.15,"credibility":0.12,"problem_focus":0.08}',
    'seed from article-zhihu.v1.yaml (M2)'
),
(
    'article/wechat', 1,
    '{"accuracy":0.25,"structure":0.15,"readability":0.15,"format_compliance":0.10,"title_potential":0.12,"narrative":0.13,"quotability":0.05,"pacing":0.05}',
    'seed from article-wechat.v1.yaml (M2)'
)
on conflict (rubric_id, version) do nothing;

-- +goose Down
delete from weight_sets
where version = 1 and rubric_id in ('article/xiaohongshu', 'article/zhihu', 'article/wechat');
alter table article_evaluations
    drop column passed,
    drop column pass_threshold,
    drop column vetoed_dimension,
    drop column weight_version,
    drop column dimension_reasons;
drop index articles_previous_unique;
alter table articles
    drop constraint articles_previous_not_self,
    drop column previous_article_id;
