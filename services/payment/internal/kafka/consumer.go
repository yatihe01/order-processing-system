package kafka

import (
	"context"
	"errors"
	"log"

	eventsv1 "orderproc/proto/gen/events/v1"
	"orderproc/services/payment/internal/store"

	segmentio "github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

const consumerGroupID = "payment-service"

// Consumer processes OrderCreated events: run the mock charge, record the
// outcome, publish the result. Delivery is at-least-once: Run uses
// FetchMessage + CommitMessages (not the auto-committing ReadMessage) so a
// message only commits after process succeeds. Kafka's per-partition commit is
// cumulative -- committing message N+1 would silently also commit a failed,
// uncommitted message N -- so a process error stops the whole loop rather than
// skipping forward; recovery is a restart, which redelivers the failed
// message. A genuinely unparseable message (unmarshal failure) is skipped and
// committed instead, since redelivering a corrupt message forever can't ever
// succeed.
type Consumer struct {
	store    *store.Store
	producer *Producer
}

func NewConsumer(s *store.Store, p *Producer) *Consumer {
	return &Consumer{store: s, producer: p}
}

// Run consumes order-created until ctx is cancelled.
func (c *Consumer) Run(ctx context.Context, brokers []string) error {
	reader := segmentio.NewReader(segmentio.ReaderConfig{
		Brokers: brokers,
		Topic:   TopicOrderCreated,
		GroupID: consumerGroupID,
	})
	defer reader.Close()

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}

		var evt eventsv1.OrderCreated
		if err := proto.Unmarshal(msg.Value, &evt); err != nil {
			log.Printf("order-created: unmarshal: %v", err)
			if err := reader.CommitMessages(ctx, msg); err != nil {
				return err
			}
			continue
		}

		if err := c.process(ctx, &evt); err != nil {
			log.Printf("order-created: process order %q: %v (not committed, will redeliver)", evt.GetOrderId(), err)
			return err
		}
		if err := reader.CommitMessages(ctx, msg); err != nil {
			return err
		}
	}
}

// process runs the mock charge and publishes the result, exactly once per
// order even under redelivery. TODO(you): implement.
//
// Proposed design:
//
//  1. Attempt store.RecordPayment first (generate a fresh payment_id, run
//     charge.Run, insert). On plain success, publish the corresponding event
//     with that payment_id -- today's behavior, unchanged.
//  2. On a duplicate-key error from RecordPayment (mirrors Reserve's exact
//     check: errors.As(err, &mysqlErr) && mysqlErr.Number == 1062) -- this
//     order was already charged, by a previous delivery of this same
//     OrderCreated. Call store.GetPayment(ctx, orderID) to get the *existing*
//     payment_id and status, and republish using that payment_id (not a new
//     one). This also plugs Phase 3's dual-write gap: if the first delivery's
//     publish failed after its DB commit succeeded, this redelivery is what
//     actually gets the event out.
//
// Explicit mock-vs-real-gateway tradeoff: the payments table doesn't persist
// the decline reason, so on a FAILED conflict this design recomputes it by
// re-running charge.Run (deterministic -- same items in, same reason out).
// That's only valid because today's charge is a pure mock with no external
// side effect. It would be wrong against a real gateway for two reasons: (1) a
// real decline reason can depend on gateway-side state (fraud score, rate
// limits) that isn't a pure function of the order's items, so recomputing it
// could produce a different answer than what actually happened; (2)
// "recompute" would mean calling the gateway again, which is a real
// side-effecting charge attempt -- exactly what idempotency is supposed to
// prevent. A real integration would need to persist the outcome at charge
// time instead of ever recomputing it.
func (c *Consumer) process(ctx context.Context, evt *eventsv1.OrderCreated) error {
	return errors.New("not implemented")
}
