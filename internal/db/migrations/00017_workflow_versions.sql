-- +goose Up
-- SPEC-010: immutable registry for workflow/agent/prompt/rubric/weight/model versions.
create table if not exists workflow_versions (
    id          uuid primary key default gen_random_uuid(),
    kind        text not null check (kind in ('workflow', 'agent', 'prompt', 'rubric', 'weight', 'model')),
    name        text not null,
    version     text not null,
    status      text not null default 'active' check (status in ('active', 'retired')),
    metadata    jsonb not null default '{}',
    sha256      text not null check (length(sha256) = 64),
    created_at  timestamptz not null default now(),
    retired_at  timestamptz,
    unique (kind, name, version)
);
create index if not exists workflow_versions_lookup_idx
    on workflow_versions(kind, name, version, status);

-- The seed is idempotent. New versions are registered through the API;
-- historical rows are never replaced by a later deployment.
insert into workflow_versions (kind, name, version, metadata, sha256)
values
    ('workflow', 'content-production', 'v1', '{}', '370512c7726c6e927bdb108dc836afed42d68e7eb5406c14c5b199bd5e754786'),
    ('agent', 'topic-scout', 'v1', '{}', 'fbc8ab5a5dffe1c2bec3f6306facd6de696b7dc7e8a688af77fc059e562e274e'),
    ('agent', 'topic-judge', 'v1', '{}', '90f22a0a520ad3bc7ea3a356f69d2fd6a0f8b05ea319e3062155f07c5acef4be'),
    ('agent', 'article-writer', 'v1', '{}', '08e239a874d2583af96db02e739f1b394218fce055b57199dad90e3793d26d86'),
    ('agent', 'article-judge', 'v1', '{}', '9e0aa69608db786165e56824e07e492174f6612e5955fe36b3093b8042636739'),
    ('agent', 'memory-reflector', 'v1', '{}', '5b784f8efa835ec2277a3b39b09e1bb84ed5041d4291ba35ad0edf966df47dbc'),
    ('prompt', 'topic-scout', 'v1', '{}', '9adb380694f8f80df863d8a95d50c0bc428ada98d5e743d3983162c2ab10f1fb'),
    ('prompt', 'topic-judge', 'v2', '{}', '9d080dcbae553534c176818dab4aa7311262ae5dc93b56c0a18142364634b758'),
    ('prompt', 'writer-orchestrator', 'v1', '{}', 'd66bc50a06e8d8331d6453cc973bfc0d3c3ae5fba9d9da9f7334e55f10b882b5'),
    ('prompt', 'writer-outline', 'v1', '{}', 'a4238c9c81fe7e058cb937118e98037ee487d56f1979fd0e9c861f10c05cb70c'),
    ('prompt', 'writer-draft', 'v1', '{}', '5ffc3be6651a67114c715aa0abe44c75141a8c81359349fab49344fde86fd438'),
    ('prompt', 'writer-self-critic', 'v1', '{}', '45e47d03ea65d5434ab797224e6a0119accd8612a627a45bc819d3d857c2c295'),
    ('prompt', 'article-judge', 'v1', '{}', '1d45586bf705adde8e3e4770af9112e5e23f796bb10c3bb58ab3851c87f94c20'),
    ('prompt', 'memory-reflector', 'v1', '{}', '316b58c8a7fc9f7b24439c97a691cd1726d217a573470bed316bca7ed85bd27a'),
    ('rubric', 'topic', 'v1', '{}', '5ea5eabcbbb7c1955012176682fe0e494385530c5ffd1d51343cc13ab7908'),
    ('rubric', 'topic', 'v2', '{}', '80316b1876cf8161ef253ac93356a74dbe709ae5698785ada8a5e7992f93dce5'),
    ('rubric', 'article/xiaohongshu', 'v1', '{}', '1db948e5c0f16173fd8b2f0d3fab3302a66d769b217c5824f6346036ead37f5e'),
    ('rubric', 'article/zhihu', 'v1', '{}', 'efd6f73b936e9e6885ad3a72acacec1ae8a34e002e71cafe6e1db94638b1a07d'),
    ('rubric', 'article/wechat', 'v1', '{}', '5a4238488c67f5602697a622c9e687206149eb4c173ecfe1ab73698a91b0bb01'),
    ('weight', 'topic', '1', '{}', '6a76290d5b55a6eba2dfb9f42a4e4ac29b903d59e10e11eb94424bb07a0bd6ed'),
    ('weight', 'article/xiaohongshu', '1', '{}', '580f77852c7ebc1d756d20be6b5c4b9c2ac18ec976fb7c87c8b610baa8f90bac'),
    ('weight', 'article/zhihu', '1', '{}', '661331cab4268da714c29bb56f7b6767f05a2cffcdd861993709724b2c59901c'),
    ('weight', 'article/wechat', '1', '{}', '7a3ca7c5992d98f03a5da26e444e32154201c10ecc909f1efccb1ed4f6ff9b3b'),
    ('model', 'siliconflow/deepseek-ai/DeepSeek-V3', 'v1', '{}', '133a87581ad83dd04e1d90d056dcdcaad8565675020df6b8d57a0e5531420f35'),
    ('model', 'vtrix/gpt-5.6-sol', 'v1', '{}', '12932c46a64c2bae640a5e1a9ab445b0d4afa8f88f72278e1ebfbb24b9935216')
on conflict (kind, name, version) do nothing;

-- +goose Down
drop table if exists workflow_versions;
