# CLAUDE.md — Distributed Order Processing System

> Project context for Claude Code. Read this at the start of every session.

## What this project is

A Go microservices system that processes orders across three services communicating over
**gRPC (synchronous)** and **Kafka (asynchronous)**, backed by **MySQL** and **Redis**, with
full **observability** (metrics + distributed tracing). It is a portfolio project targeting
backend / infrastructure internships at companies like ByteDance/TikTok and Shopee, whose job
descriptions center on Go, RPC frameworks, high-concurrency distributed systems, and the
MySQL/Redis/Kafka stack.

The goal is **not** feature count. The goal is that I can defend every design decision in an
interview. A smaller system I deeply understand beats a larger one I generated.

## How I want you to work with me — READ THIS FIRST

This is the most important section. My learning is the point of the project.

1. **Default to Plan mode** for any new service, schema, or cross-cutting concern. Show me the
   plan before writing code.
2. **When you reach a design decision, STOP and ask me.** Do not silently pick an approach.
   Present 2–3 options with their tradeoffs and wait for my choice. Design decisions include:
   consistency models, sync-vs-async boundaries, schema and indexing, concurrency control,
   caching strategy, idempotency, Kafka partitioning, and retry/timeout policy.
3. **After I decide, record it** in the Decision Log below (my choice + my one-line reasoning).
4. **I write the hard parts.** For the concurrency, consistency, and idempotency logic
   specifically: propose the design, then let me attempt the implementation myself. Review my
   code and push back — don't write it for me unless I explicitly ask.
5. **Small diffs.** One service or one concern at a time. Never scaffold multiple services in a
   single pass. Explain *why*, not just *what*, in your summaries.
6. **Interview lens.** After each meaningful chunk, tell me the one or two interview questions
   this code lets me answer, so I know what story I'm building.

## Architecture

Three services. One synchronous path (gRPC) and one asynchronous path (Kafka events).

```
                    ┌──────────────┐   gRPC: Reserve/Release   ┌──────────────────┐
   client ─gRPC──▶  │   Order svc  │ ────────────────────────▶ │  Inventory svc   │
                    │  (orders DB) │                            │ (inventory DB +  │
                    └──────┬───────┘ ◀───────────────────────── │   Redis cache)   │
                           │            reserve result          └──────────────────┘
                           │
                     Kafka │ publishes OrderCreated
                           ▼
                    ┌──────────────┐   Kafka: PaymentCompleted / PaymentFailed
                    │  Payment svc │ ───────────────────────────────┐
                    │ (payments DB)│                                 │
                    └──────────────┘                                 ▼
                                                       Order svc consumes result,
                                                       updates order status
```

- **Order service** — public entrypoint. `CreateOrder` (gRPC) synchronously calls Inventory to
  reserve stock, writes a `PENDING` order to MySQL, publishes `OrderCreated` to Kafka, returns
  the order id. Later consumes `PaymentCompleted`/`PaymentFailed` and updates order status.
- **Inventory service** — `Reserve` / `Release` (gRPC, synchronous because the order needs an
  immediate answer). Owns the inventory table; uses Redis for hot stock reads. Consumes
  compensation events to release stock on failure.
- **Payment service** — consumes `OrderCreated` (async, because payment is slow and retryable),
  processes a mock charge, publishes `PaymentCompleted` or `PaymentFailed`.

**Why this split matters (and what I should be able to defend):** the Order→Inventory call is
synchronous because we cannot accept an order without knowing stock exists *now*. The
Order→Payment path is asynchronous because payment latency should not block order acceptance,
and Kafka buffers/retries if the payment service is down. This is a **saga**: reserve → charge →
confirm, with compensation (release stock, cancel order) on failure.

## Tech stack (fixed)

| Concern         | Choice                        | Note                                             |
|-----------------|-------------------------------|--------------------------------------------------|
| Language        | Go                            | The dominant language in the target JDs          |
| RPC             | gRPC + Protocol Buffers       | Inter-service synchronous calls                  |
| Messaging       | Kafka                         | Async events; named explicitly in the JDs        |
| Relational DB   | **MySQL**                     | Named in Shopee/TikTok JDs — not Postgres        |
| Cache           | Redis                         | Hot inventory reads                              |
| Local infra     | Docker Compose                | Kafka, MySQL, Redis, Prometheus, Grafana, Jaeger |
| Metrics         | Prometheus + Grafana          | RED metrics per service                          |
| Tracing         | OpenTelemetry → Jaeger        | Traces across the gRPC + Kafka boundary          |
| Load testing    | k6 or ghz                     | For the throughput/bottleneck story              |

## Decisions I must make myself

These are open on purpose. When we reach each one, walk me through the options — don't pre-fill
the answer. I'll record what I chose and why in the Decision Log.

1. **Saga style** — choreography (services react to events) vs orchestration (Order service
   drives the flow explicitly). Tradeoffs: coupling, visibility, complexity.
2. **Inventory concurrency control** — `SELECT ... FOR UPDATE` row lock vs optimistic version
   column vs authoritative atomic decrement in Redis. This is the core correctness question.
3. **Redis role** — cache-aside with MySQL authoritative, vs Redis as the source of truth for
   stock counts. Affects consistency and failure behavior.
4. **Idempotency** — Kafka is at-least-once, so consumers must dedupe redeliveries (must not
   double-charge or double-decrement). Idempotency-key table? Dedupe on event id? Where stored?
5. **Kafka partitioning key** — by order id, product id, or user id? Determines ordering
   guarantees and hot-partition risk.
6. **Retry & timeout policy** — deadlines and retry behavior on the Order→Inventory gRPC call;
   what's safe to retry given idempotency.
7. **Schema + indexes** — table design and which indexes the read paths actually need.
8. **What to observe** — which RED metrics per service, and where trace spans start/end.

## Decision Log

Fill this in as we go. One line of reasoning each — this is my interview cheat sheet.

| # | Decision            | What I chose | Why |
|---|---------------------|--------------|-----|
| 1 | Saga style          |              |     |
| 2 | Inventory concurrency |            |     |
| 3 | Redis role          |              |     |
| 4 | Idempotency         |              |     |
| 5 | Kafka partition key |              |     |
| 6 | Retry/timeout       |              |     |
| 7 | Schema/indexes      | ULID as `orders.order_id`/PK; normalized `order_items` (FK, PK `(order_id, product_id)`); no index on `user_id` yet | ULID is client-facing id == PK (no secondary lookup) but time-ordered so inserts don't fragment InnoDB's clustered index like random UUIDv4; normalized items leave room to query by `product_id` later; `user_id` index deferred until a read path needs it |
| 8 | Observability       |              |     |

## Conventions

- **Layout:** one Go module per service under `services/<name>/`, shared protobuf under
  `proto/`, generated code committed. Keep `cmd/`, `internal/` structure per service.
- **Errors:** wrap with `fmt.Errorf("...: %w", err)`; return gRPC status codes at the boundary,
  not raw errors.
- **Context:** every service call and DB query takes a `context.Context` with a deadline.
- **Config:** environment variables, read once at startup; no hardcoded hosts/ports.
- **Tests:** table-driven unit tests for domain logic; at least one integration test per service
  that runs against the Docker Compose infra.
- **Commits:** small and scoped; message says what changed and why.

## Commands

Fill these in as the Makefile grows.

```
make tools            # install protoc-gen-go / protoc-gen-go-grpc / golang-migrate
make proto            # regenerate gRPC/protobuf code
make up               # start infra via docker compose (Kafka, MySQL, Redis)
make down             # stop infra
make migrate-order    # apply Order service MySQL migrations
make run-order        # run the order service
make test             # unit tests (no infra required)
make test-integration # integration tests against the Docker Compose infra
make load             # run the load test                (TODO)
```

## Out of scope (say no if I drift here)

Real payment gateway, auth/user management, a frontend, Kubernetes deployment, and
multi-region concerns. If I start reaching for these before the core saga + observability +
load-test story is solid, remind me they're out of scope.
