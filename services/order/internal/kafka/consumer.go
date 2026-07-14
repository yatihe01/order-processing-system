package kafka

import (
	"context"
	"errors"
	"log"

	eventsv1 "orderproc/proto/gen/events/v1"
	"orderproc/services/order/internal/store"

	segmentio "github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

const (
	TopicPaymentCompleted = "payment-completed"
	TopicPaymentFailed    = "payment-failed"
	consumerGroupID       = "order-service"
)

// ResultConsumer updates order status from payment result events. Delivery is
// at-least-once with auto-commit; deduping redeliveries is Phase 4.
type ResultConsumer struct {
	store *store.Store
}

func NewResultConsumer(s *store.Store) *ResultConsumer {
	return &ResultConsumer{store: s}
}

func newReader(brokers []string, topic string) *segmentio.Reader {
	return segmentio.NewReader(segmentio.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: consumerGroupID,
	})
}

// RunPaymentCompleted consumes payment-completed until ctx is cancelled.
func (c *ResultConsumer) RunPaymentCompleted(ctx context.Context, brokers []string) error {
	reader := newReader(brokers, TopicPaymentCompleted)
	defer reader.Close()

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}

		var evt eventsv1.PaymentCompleted
		if err := proto.Unmarshal(msg.Value, &evt); err != nil {
			log.Printf("payment-completed: unmarshal: %v", err)
			continue
		}
		if err := c.store.UpdateStatus(ctx, evt.GetOrderId(), "CONFIRMED"); err != nil {
			log.Printf("payment-completed: update status for order %q: %v", evt.GetOrderId(), err)
		}
	}
}

// RunPaymentFailed consumes payment-failed until ctx is cancelled.
func (c *ResultConsumer) RunPaymentFailed(ctx context.Context, brokers []string) error {
	reader := newReader(brokers, TopicPaymentFailed)
	defer reader.Close()

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}

		var evt eventsv1.PaymentFailed
		if err := proto.Unmarshal(msg.Value, &evt); err != nil {
			log.Printf("payment-failed: unmarshal: %v", err)
			continue
		}
		if err := c.store.UpdateStatus(ctx, evt.GetOrderId(), "FAILED"); err != nil {
			log.Printf("payment-failed: update status for order %q: %v", evt.GetOrderId(), err)
		}
	}
}
