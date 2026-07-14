package kafka

import (
	"context"
	"errors"
	"log"

	eventsv1 "orderproc/proto/gen/events/v1"
	"orderproc/services/payment/internal/charge"
	"orderproc/services/payment/internal/store"

	"github.com/oklog/ulid/v2"
	segmentio "github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

const consumerGroupID = "payment-service"

// Consumer processes OrderCreated events: run the mock charge, record the
// outcome, publish the result. Delivery is at-least-once with auto-commit;
// deduping redeliveries (so a redelivered OrderCreated doesn't double-charge)
// is Phase 4.
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
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}

		var evt eventsv1.OrderCreated
		if err := proto.Unmarshal(msg.Value, &evt); err != nil {
			log.Printf("order-created: unmarshal: %v", err)
			continue
		}

		c.process(ctx, &evt)
	}
}

func (c *Consumer) process(ctx context.Context, evt *eventsv1.OrderCreated) {
	orderID := evt.GetOrderId()
	paymentID := ulid.Make().String()

	ok, reason := charge.Run(evt.GetItems())
	status := "COMPLETED"
	if !ok {
		status = "FAILED"
	}

	if err := c.store.RecordPayment(ctx, paymentID, orderID, status); err != nil {
		log.Printf("order-created: record payment for order %q: %v", orderID, err)
		return
	}

	if ok {
		if err := c.producer.PublishPaymentCompleted(ctx, orderID, paymentID); err != nil {
			log.Printf("order-created: publish PaymentCompleted for order %q: %v", orderID, err)
		}
		return
	}
	if err := c.producer.PublishPaymentFailed(ctx, orderID, reason); err != nil {
		log.Printf("order-created: publish PaymentFailed for order %q: %v", orderID, err)
	}
}
