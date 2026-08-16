-- +goose Up
-- M2 人工终稿仍不覆盖不可变 Article；Core 在登记 Publication 时计算并保存修改比例。
alter table publications
    add column edit_ratio numeric(7,6)
        check (edit_ratio is null or (edit_ratio >= 0 and edit_ratio <= 1));

-- 同一个平台侧 ID/链接不能重复登记；允许历史空值继续存在。
create unique index publications_platform_post_unique
    on publications (platform, platform_post_id)
    where platform_post_id is not null and platform_post_id <> '';

-- +goose Down
drop index publications_platform_post_unique;
alter table publications drop column edit_ratio;
