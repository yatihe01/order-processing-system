package kafka

import (
	"context"
	"errors"
	"fmt"
	"log"

	eventsv1 "orderproc/proto/gen/events/v1"
	"orderproc/services/payment/internal/charge"
	"orderproc/services/payment/internal/store"

	"github.com/go-sql-driver/mysql"
	"github.com/oklog/ulid/v2"
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
// order even under redelivery.
//
// RecordPayment is attempted first with a freshly generated payment_id. A
// duplicate-key error on it (mirrors Reserve's exact check:
// errors.As(err, &mysqlErr) && mysqlErr.Number == 1062) means this order was
// already charged by a previous delivery of this same OrderCreated: look up
// the existing payment_id/status via GetPayment and republish using that
// payment_id instead of the fresh one. This also plugs Phase 3's dual-write
// gap -- if the first delivery's publish failed after its DB commit
// succeeded, this redelivery is what actually gets the event out.
//
// Explicit mock-vs-real-gateway tradeoff: the payments table doesn't persist
// the decline reason, so on a FAILED conflict this reuses the `reason`
// already computed above by charge.Run rather than storing/re-fetching it --
// valid only because today's charge is a pure, deterministic mock with no
// external side effect (same items in, same reason out). It would be wrong
// against a real gateway for two reasons: (1) a real decline reason can
// depend on gateway-side state (fraud score, rate limits) that isn't a pure
// function of the order's items, so recomputing it could produce a different
// answer than what actually happened; (2) recomputing would mean calling the
// gateway again, which is a real side-effecting charge attempt -- exactly
// what idempotency is supposed to prevent. A real integration would need to
// persist the outcome at charge time instead of ever recomputing it.
func (c *Consumer) process(ctx context.Context, evt *eventsv1.OrderCreated) error {
	orderID := evt.GetOrderId()

	ok, reason := charge.Run(evt.GetItems())
	status := "COMPLETED"
	if !ok {
		status = "FAILED"
	}

	paymentID := ulid.Make().String()
	if err := c.store.RecordPayment(ctx, paymentID, orderID, status); err != nil {
		var mysqlErr *mysql.MySQLError
		if !errors.As(err, &mysqlErr) || mysqlErr.Number != 1062 {
			return fmt.Errorf("record payment for order %q: %w", orderID, err)
		}

		// Already charged by a previous delivery of this same OrderCreated.
		// charge.Run is deterministic, so status/reason computed above already
		// match what was originally stored -- but the payment_id must be the
		// one actually persisted, not the fresh one generated above.
		existingPaymentID, existingStatus, getErr := c.store.GetPayment(ctx, orderID)
		if getErr != nil {
			return fmt.Errorf("lookup existing payment for order %q: %w", orderID, getErr)
		}
		paymentID = existingPaymentID
		status = existingStatus
	}

	if status == "COMPLETED" {
		if err := c.producer.PublishPaymentCompleted(ctx, orderID, paymentID); err != nil {
			return fmt.Errorf("publish PaymentCompleted for order %q: %w", orderID, err)
		}
		return nil
	}
	if err := c.producer.PublishPaymentFailed(ctx, orderID, reason); err != nil {
		return fmt.Errorf("publish PaymentFailed for order %q: %w", orderID, err)
	}
	return nil
}
