-- +goose Up
-- SPEC-010: associate generated article versions with their workflow run so
-- replay output cannot be mistaken for a parent run's article.
alter table articles add column if not exists correlation_id uuid;
create index if not exists articles_correlation_idx on articles(correlation_id, topic_id, platform, version desc)
    where correlation_id is not null;

-- +goose Down
drop index if exists articles_correlation_idx;
alter table articles drop column if exists correlation_id;
