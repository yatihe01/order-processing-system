PROTO_DIR := proto
GEN_DIR := proto/gen
COMPOSE_FILE := deploy/docker-compose.yml

.PHONY: tools proto up down run-order test load

tools:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

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

run-order: # TODO: Phase 1
	@echo "order service not implemented yet"

test: # TODO: Phase 1
	@echo "no tests yet"

load: # TODO: Phase 7
	@echo "no load test yet"
