// Package queue 是 pgmq 的薄封装（ADR-003）。
// 关键约定：Send 接收 pgx.Tx —— 入队必须与业务状态变更同事务提交，
// 保证"状态推进"与"任务投递"的原子性（SPEC-001 §3）。
package queue

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Name 是 pgmq 队列名。合法值以 scholar-shared/schemas/queues.json 为准；
// 常量在此镜像声明，CI 后续增加与注册表的一致性校验（M1）。
type Name string

const (
	SourceFetch     Name = "source_fetch"
	TopicScout      Name = "topic_scout"
	TopicEvaluate   Name = "topic_evaluate"
	ArticleWrite    Name = "article_write"
	ArticleEvaluate Name = "article_evaluate"
	MemoryReflect   Name = "memory_reflect"
)

// Send 在事务内投递一条 job。payload 必须是 shared 契约中的 job 类型。
func Send(ctx context.Context, tx pgx.Tx, q Name, payload any) (msgID int64, err error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return 0, fmt.Errorf("queue send %s: marshal payload: %w", q, err)
	}
	if err := tx.QueryRow(ctx, "select pgmq.send($1, $2::jsonb)", string(q), body).Scan(&msgID); err != nil {
		return 0, fmt.Errorf("queue send %s: %w", q, err)
	}
	return msgID, nil
}
