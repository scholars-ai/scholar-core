# 工具全部通过 go run 固定版本调用，无需本机安装
SHARED ?= ../scholar-shared
GOOSE_VERSION := v3.24.2
SQLC_VERSION := v1.27.0
OAPI_CODEGEN_VERSION := v2.4.1

.PHONY: build test vet generate migrate-up migrate-down run

build:
	go build ./...

test:
	go test ./...

vet:
	go vet ./...

run:
	go run ./cmd/core

## 重新生成 API server（依赖同级目录的 scholar-shared 检出）与 sqlc 代码
generate:
	go run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@$(OAPI_CODEGEN_VERSION) \
		--config oapi-codegen.yaml $(SHARED)/openapi/core.yaml
	go run github.com/sqlc-dev/sqlc/cmd/sqlc@$(SQLC_VERSION) generate

## 迁移（DATABASE_URL 必须已设置）
migrate-up:
	go run github.com/pressly/goose/v3/cmd/goose@$(GOOSE_VERSION) \
		-dir internal/db/migrations postgres "$(DATABASE_URL)" up

migrate-down:
	go run github.com/pressly/goose/v3/cmd/goose@$(GOOSE_VERSION) \
		-dir internal/db/migrations postgres "$(DATABASE_URL)" down
