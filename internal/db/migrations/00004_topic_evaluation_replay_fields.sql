-- +goose Up
-- 评分回放需要记录当时生效的权重版本，以及是否触发了一票否决。
alter table topic_evaluations
    add column weight_version int,
    add column vetoed_dimension text;

-- +goose Down
alter table topic_evaluations
    drop column vetoed_dimension,
    drop column weight_version;
