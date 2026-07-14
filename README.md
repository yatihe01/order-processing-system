# Distributed Order Processing System

A Go microservices system that processes orders across three services communicating over
**gRPC** (synchronous) and **Kafka** (asynchronous), backed by **MySQL** and **Redis**, with
**observability** (metrics + distributed tracing). This is a portfolio project — the goal is a
system small enough to defend every design decision in an interview, not feature count.

> Design rationale and open decisions live in [CLAUDE.md](CLAUDE.md) (architecture, tech stack,
> decision log). Build sequencing lives in [ROADMAP.md](ROADMAP.md). This README will grow into
> the full write-up (numbers, tradeoffs, "what I'd change at scale") in the final phase.

## Status

- **Phase 0 — Foundations**: done. Proto contracts, Docker Compose infra, Go workspace.
- **Phase 1 — Order service**: done. `CreateOrder` / `GetOrder` over gRPC, backed by MySQL.
- **Phase 2 — Inventory service + synchronous `Reserve`**: scaffolding done; the row-lock
  reservation logic itself (`Reserve`/`Release`) is stubbed pending implementation.
- **Phase 3 — Async messaging (Payment service + Kafka)**: done. `Order` publishes
  `OrderCreated`, `Payment` mock-charges and publishes the result, `Order` consumes it and
  updates status. Can't run end to end yet since it's downstream of Phase 2's still-stubbed
  `Reserve`/`Release`.

See [ROADMAP.md](ROADMAP.md) for the full phase breakdown.

## Architecture

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

The Order→Inventory call is synchronous because an order can't be accepted without knowing
stock exists *now*. The Order→Payment path is asynchronous so payment latency doesn't block
order acceptance, and Kafka buffers/retries if Payment is down. This is a saga:
reserve → charge → confirm, with compensation (release stock, cancel order) on failure.

## Tech stack

| Concern       | Choice                  |
|---------------|--------------------------|
| Language      | Go                      |
| RPC           | gRPC + Protocol Buffers |
| Messaging     | Kafka                   |
| Relational DB | MySQL                   |
| Cache         | Redis                   |
| Local infra   | Docker Compose          |
| Metrics       | Prometheus + Grafana    |
| Tracing       | OpenTelemetry → Jaeger  |
| Load testing  | k6 or ghz               |

## Repo layout

```
proto/           # .proto contracts + generated Go code (proto/gen)
services/        # one Go module per service (cmd/, internal/ per service)
  order/         # Order service — implemented
  inventory/     # Inventory service — scaffolded, Reserve/Release stubbed
  payment/       # Payment service — implemented
deploy/          # docker-compose.yml + MySQL init scripts
```

## Running locally

```
make up                 # start Kafka, MySQL, Redis via docker compose
make migrate-order      # apply Order service MySQL migrations
make migrate-inventory  # apply Inventory service MySQL migrations
make migrate-payment    # apply Payment service MySQL migrations
make run-order          # run the Order service
make run-inventory      # run the Inventory service
make run-payment        # run the Payment service
make test                 # unit tests (no infra required)
make test-integration     # integration tests against the Docker Compose infra
make down                 # stop infra
```
