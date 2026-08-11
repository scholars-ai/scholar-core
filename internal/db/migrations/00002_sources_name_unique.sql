-- +goose Up
-- 信源名唯一：信源清单以 scholar-agents/config/sources.yaml 为准，按 name 幂等 upsert
-- （seed-sources 命令依赖该约束）。同时便于人工在 client 上按名识别信源。
alter table sources add constraint sources_name_key unique (name);

-- +goose Down
alter table sources drop constraint sources_name_key;
