# ROADMAP.md — Build Plan

Phased so each step ends in something working and each phase names the **design decision** you'll
make and the **interview question** it lets you answer. Do them in order — later phases assume the
earlier scaffolding exists. Check boxes as you go.

A realistic pace is one phase every day or two of focused work. Phases 0–4 are the core system;
5–8 are what turn it from "it runs" into "I can talk about it."

---

## Phase 0 — Foundations
**Goal:** repo, local infra, and the shared contract exist.

- [x] Init the repo, Go workspace, and directory layout (`proto/`, `services/`, `deploy/`).
- [x] Write a `docker-compose.yml` for Kafka, MySQL, Redis (add Prometheus/Grafana/Jaeger in Phase 6).
- [x] Write the `.proto` files for the Order and Inventory services and the Kafka event schemas.
- [x] `make proto` generates Go code; commit the generated output.

**Decision:** none yet — but design the proto contracts carefully; they're your API surface.
**Interview story:** "Here's how I versioned my service contracts and what I'd change to evolve them."

---

## Phase 1 — First service, end to end
**Goal:** the Order service accepts a `CreateOrder` gRPC call and writes a `PENDING` order to MySQL.

- [x] Order service skeleton: gRPC server, config from env, graceful shutdown.
- [x] MySQL connection + `orders` table + migration.
- [x] Implement `CreateOrder`: validate, insert `PENDING`, return order id.
- [ ] One integration test hitting the real DB via Docker Compose. (written, not yet run —
      no Docker in the dev sandbox; run `make up && make migrate-order && make test-integration`
      locally to confirm and check this box)

**Decision #7 (schema/indexes):** design the `orders` table and its indexes.
**Interview story:** "Walk me through a request from gRPC entry to DB write, including the deadline."

---

## Phase 2 — Second service + synchronous gRPC call
**Goal:** Order synchronously reserves stock from Inventory before accepting.

- [x] Inventory service: `Reserve` / `Release` gRPC, `inventory` table.
- [x] Order service calls `Inventory.Reserve` inside `CreateOrder`; reject if reservation fails.
- [ ] Implement reservation under concurrent orders — **you write this part.** (`Reserve`/`Release`
      in `services/inventory/internal/store/store.go` are stubbed with the row-lock approach
      spelled out in comments — fill them in, then write the concurrent test below.)

**Decision #2 (concurrency control):** row lock vs optimistic version vs Redis atomic decrement.
Have Claude lay out the three; you pick and implement, then have it review for race conditions.
**Interview story:** "How do you prevent overselling when 100 orders hit the same SKU at once?"
Write a concurrent test that proves your choice holds.

---

## Phase 3 — Asynchronous messaging
**Goal:** payment happens off the request path via Kafka.

- [x] Order service publishes `OrderCreated` to Kafka after reserving + persisting.
- [x] Payment service consumes `OrderCreated`, processes a mock charge, publishes
      `PaymentCompleted` / `PaymentFailed`.
- [x] Order service consumes the result and updates order status.
      (Not run live yet — no Docker in the dev sandbox, and Inventory's `Reserve`/`Release` are
      still stubbed from Phase 2, so `CreateOrder` can't reach the publish step end to end until
      those are implemented. Verify with `make up`, all three `make migrate-*`, then
      `run-inventory && run-order && run-payment` once Reserve/Release are filled in.)

**Decision #5 (partition key):** choose the Kafka partition key and justify the ordering it gives.
**Interview story:** "Why Kafka between order and payment instead of a gRPC call? What happens if
the payment service is down for five minutes?"

---

## Phase 4 — Consistency & idempotency (the hard one)
**Goal:** the saga is correct even with retries and partial failures.

- [ ] Handle `PaymentFailed`: release the inventory reservation, mark the order `CANCELLED`
      (the compensation path).
- [ ] Make the Payment consumer idempotent — a redelivered `OrderCreated` must not double-charge.
- [ ] Make the Order consumer idempotent for `PaymentCompleted`.
- [ ] Test: kill a consumer mid-flow and confirm no double effects on restart.

**Decision #1 (saga style) + #4 (idempotency):** choreography vs orchestration, and how you dedupe.
**Interview story:** "Kafka is at-least-once — how did you stop a redelivered event from charging
the customer twice?" This is the phase that most separates you from other applicants.

---

## Phase 5 — Caching
**Goal:** inventory reads come from Redis, not MySQL, on the hot path.

- [ ] Add Redis; cache stock counts on the read path.
- [ ] Handle invalidation on reserve/release.
- [ ] Measure read latency before vs after.

**Decision #3 (Redis role):** cache-aside vs Redis-authoritative — and what breaks if Redis dies.
**Interview story:** "What's your cache invalidation strategy and what's the failure mode?"

---

## Phase 6 — Observability
**Goal:** you can see what the system is doing under load.

- [ ] Add Prometheus + Grafana; export RED metrics (rate, errors, duration) per service.
- [ ] Add OpenTelemetry tracing; propagate the trace across the gRPC call **and** the Kafka event.
- [ ] Build one Grafana dashboard and confirm a trace spans all three services in Jaeger.

**Decision #8 (what to observe):** which metrics matter and where spans begin/end.
**Interview story:** "Show me a trace of one order across all three services." Propagating context
through Kafka (not just gRPC) is a detail interviewers notice.

---

## Phase 7 — Load test & the bottleneck story
**Goal:** a quantitative narrative: measure → find bottleneck → fix → re-measure.

- [ ] Load test `CreateOrder` with k6 or ghz; record throughput and p50/p99 latency.
- [ ] Use your dashboards/traces to find the first bottleneck (connection pool? lock contention?
      serialization? a sync call that should be async?).
- [ ] Fix one thing, re-run, record the delta. Repeat once more.
- [ ] Write the numbers down.

**Decision:** which bottleneck to attack first, based on evidence not guesswork.
**Interview story:** "I pushed it to X orders/sec; the bottleneck was Y; I did Z and got to X'."
This quantitative story is gold and pairs well with your low-level/PCAP background.

---

## Phase 8 — README & write-up
**Goal:** the repo reads well to whoever screens it.

- [ ] Architecture diagram + the request/event flow.
- [ ] Copy your Decision Log into the README as "Design decisions and tradeoffs."
- [ ] The load-test numbers and the bottleneck story.
- [ ] "What I'd do differently at 100x scale" — shows you know the current design's limits.

**Interview story:** the README *is* your talking points. If you can't write the tradeoff, you
don't understand it yet — go back to that phase.

---

## If you run short on time

Phases 0–4 alone are a strong project: Go + gRPC + Kafka + MySQL + a correct saga with
idempotency. Add Phase 6 (observability) and Phase 7 (load test) next — together they produce the
two stories interviewers probe hardest. Phase 5 (caching) is the most droppable.
