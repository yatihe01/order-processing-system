PROTO_DIR := proto
GEN_DIR := proto/gen
COMPOSE_FILE := deploy/docker-compose.yml
ORDER_MYSQL_DSN ?= root:root@tcp(localhost:3306)/order_db?parseTime=true
ORDER_MIGRATE_DSN ?= mysql://root:root@tcp(localhost:3306)/order_db
INVENTORY_MYSQL_DSN ?= root:root@tcp(localhost:3306)/inventory_db?parseTime=true
INVENTORY_MIGRATE_DSN ?= mysql://root:root@tcp(localhost:3306)/inventory_db
PAYMENT_MYSQL_DSN ?= root:root@tcp(localhost:3306)/payment_db?parseTime=true
PAYMENT_MIGRATE_DSN ?= mysql://root:root@tcp(localhost:3306)/payment_db
KAFKA_BROKERS ?= localhost:9092

.PHONY: tools proto up down run-order run-inventory run-payment migrate-order migrate-inventory migrate-payment test test-integration load

tools:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	go install github.com/golang-migrate/migrate/v4/cmd/migrate@latest

proto:
	protoc \
		--proto_path=$(PROTO_DIR) \
		--go_out=$(GEN_DIR) --go_opt=paths=source_relative \
		--go-grpc_out=$(GEN_DIR) --go-grpc_opt=paths=source_relative \
		$(PROTO_DIR)/order/v1/order.proto \
		$(PROTO_DIR)/inventory/v1/inventory.proto \
		$(PROTO_DIR)/events/v1/events.proto
	cd $(GEN_DIR) && go mod tidy

up:
	docker compose -f $(COMPOSE_FILE) up -d

down:
	docker compose -f $(COMPOSE_FILE) down

migrate-order:
	migrate -path services/order/migrations -database "$(ORDER_MIGRATE_DSN)" up

migrate-inventory:
	migrate -path services/inventory/migrations -database "$(INVENTORY_MIGRATE_DSN)" up

migrate-payment:
	migrate -path services/payment/migrations -database "$(PAYMENT_MIGRATE_DSN)" up

run-order:
	ORDER_MYSQL_DSN="$(ORDER_MYSQL_DSN)" ORDER_KAFKA_BROKERS="$(KAFKA_BROKERS)" go run ./services/order/cmd/order

run-inventory:
	INVENTORY_MYSQL_DSN="$(INVENTORY_MYSQL_DSN)" go run ./services/inventory/cmd/inventory

run-payment:
	PAYMENT_MYSQL_DSN="$(PAYMENT_MYSQL_DSN)" PAYMENT_KAFKA_BROKERS="$(KAFKA_BROKERS)" go run ./services/payment/cmd/payment

test:
	go test ./proto/gen/... ./services/order/... ./services/inventory/... ./services/payment/...

test-integration:
	ORDER_MYSQL_DSN="$(ORDER_MYSQL_DSN)" INVENTORY_MYSQL_DSN="$(INVENTORY_MYSQL_DSN)" PAYMENT_MYSQL_DSN="$(PAYMENT_MYSQL_DSN)" \
		go test -tags=integration ./services/order/... ./services/inventory/... ./services/payment/...

load: # TODO: Phase 7
	@echo "no load test yet"
