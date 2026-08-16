// Package db 提供连接池构造：注册全部自定义 enum 类型（含数组形态），
// 否则 pgx 无法编解码 platform[] 等自定义类型（unknown OID）。
package db

import (
	"context"
	"fmt"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	pgxvec "github.com/pgvector/pgvector-go/pgx"
)

// enumTypes 与 internal/db/migrations/00001_init.sql 中的 create type 保持一致。
var enumTypes = []string{
	"platform",
	"article_format",
	"source_type",
	"source_category",
	"raw_item_status",
	"topic_status",
	"article_status",
	"metric_source",
	"insight_kind",
	"insight_status",
	"agent_run_status",
}

func NewPool(ctx context.Context, url string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	cfg.ConnConfig.Tracer = otelpgx.NewTracer(
		otelpgx.WithDisableSQLStatementInAttributes(),
		otelpgx.WithDisableConnectionDetailsInAttributes(),
	)
	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		if err := pgxvec.RegisterTypes(ctx, conn); err != nil {
			return fmt.Errorf("register pgvector types: %w", err)
		}
		for _, name := range enumTypes {
			// 同时注册标量与数组形态（pg 数组类型名为 _name）
			for _, n := range []string{name, "_" + name} {
				t, err := conn.LoadType(ctx, n)
				if err != nil {
					return fmt.Errorf("load pg type %s: %w", n, err)
				}
				conn.TypeMap().RegisterType(t)
			}
		}
		return nil
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := otelpgx.RecordStats(pool); err != nil {
		pool.Close()
		return nil, fmt.Errorf("record pgx pool stats: %w", err)
	}
	return pool, nil
}
