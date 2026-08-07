# scholar-core

scholars-ai 业务核心（Go）：REST API、Pipeline 状态机（唯一写入口）、定时调度、指标聚合。**不碰 LLM**——一切 LLM 工作在 [scholar-agents](https://github.com/scholars-ai/scholar-agents)。架构见 [spec/SPEC-001](https://github.com/scholars-ai/spec/blob/main/specs/SPEC-001-architecture.md)。

## 技术栈

chi（路由）· sqlc（类型安全 SQL）· goose（迁移）· oapi-codegen（OpenAPI-first，契约来自 [scholar-shared](https://github.com/scholars-ai/scholar-shared)）· pgx/pgxpool · pgmq（任务队列，ADR-003）

## 结构

```
cmd/core/                 入口（graceful shutdown）
internal/api/             oapi-codegen 生成的 server 接口 + 实现
internal/config/          env 配置
internal/db/migrations/   goose 迁移（SPEC-002 全部表 + pgmq 队列创建）
internal/db/queries/      sqlc 查询源
internal/db/dbgen/        sqlc 生成物（提交入库）
internal/pipeline/        状态机（SPEC-002 §3 的合法流转表 + 测试）
internal/queue/           pgmq 薄封装（入队必须与状态变更同事务）
```

## 开发

```bash
cp .env.example .env            # 填 DATABASE_URL
make migrate-up                 # goose 迁移（含 pgmq/pgvector 扩展与队列创建）
make run                        # 起服务
curl localhost:8080/api/healthz
# 修改 API 契约（在 scholar-shared 仓库）或 SQL 后：
make generate                   # 重新生成 api.gen.go / dbgen（假设 shared 在同级目录）
```

## 约定

- **状态流转只在本服务发生**：agents 只写结果表，core 收割结果推进状态机（`internal/pipeline` 的流转表是唯一裁判）。
- **入队 = 事务的一部分**：投递 job 一律走 `queue.Send(ctx, tx, ...)`，与业务变更同 commit。
- 生成物（api.gen.go / dbgen/）提交入库；改契约先改 shared，再 `make generate`。
