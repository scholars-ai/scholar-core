# 工具全部通过 go run 固定版本调用，无需本机安装
SHARED ?= ../scholar-shared
GOOSE_VERSION := v3.24.2
SQLC_VERSION := v1.31.1
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
## 注意：@ 前缀 + 由 goose 自己读 GOOSE_DBSTRING，避免 make 回显把密码打进终端/CI 日志
migrate-up:
	@GOOSE_DRIVER=postgres GOOSE_DBSTRING="$(DATABASE_URL)" \
		go run github.com/pressly/goose/v3/cmd/goose@$(GOOSE_VERSION) \
		-dir internal/db/migrations up

migrate-down:
	@GOOSE_DRIVER=postgres GOOSE_DBSTRING="$(DATABASE_URL)" \
		go run github.com/pressly/goose/v3/cmd/goose@$(GOOSE_VERSION) \
		-dir internal/db/migrations down

migrate-status:
	@GOOSE_DRIVER=postgres GOOSE_DBSTRING="$(DATABASE_URL)" \
		go run github.com/pressly/goose/v3/cmd/goose@$(GOOSE_VERSION) \
		-dir internal/db/migrations status
