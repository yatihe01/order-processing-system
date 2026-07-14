// Package kafka publishes OrderCreated and consumes payment result events.
package kafka

import (
	"context"
	"fmt"
	"time"

	eventsv1 "orderproc/proto/gen/events/v1"
	orderv1 "orderproc/proto/gen/order/v1"
	"orderproc/services/order/internal/store"

	"github.com/oklog/ulid/v2"
	segmentio "github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

const TopicOrderCreated = "order-created"

// Producer publishes OrderCreated events, keyed by order_id (Decision #5) so every
// event for a given order lands on the same partition.
type Producer struct {
	writer *segmentio.Writer
}

func NewProducer(brokers []string) *Producer {
	return &Producer{
		writer: &segmentio.Writer{
			Addr:     segmentio.TCP(brokers...),
			Topic:    TopicOrderCreated,
			Balancer: &segmentio.Hash{},
		},
	}
}

func (p *Producer) Close() error {
	return p.writer.Close()
}

func (p *Producer) PublishOrderCreated(ctx context.Context, order store.Order) error {
	items := make([]*orderv1.OrderItem, 0, len(order.Items))
	for _, i := range order.Items {
		items = append(items, &orderv1.OrderItem{ProductId: i.ProductID, Quantity: i.Quantity})
	}

	evt := &eventsv1.OrderCreated{
		EventId:   ulid.Make().String(),
		OrderId:   order.OrderID,
		UserId:    order.UserID,
		Items:     items,
		Timestamp: time.Now().Unix(),
	}

	value, err := proto.Marshal(evt)
	if err != nil {
		return fmt.Errorf("kafka: marshal OrderCreated: %w", err)
	}

	if err := p.writer.WriteMessages(ctx, segmentio.Message{
		Key:   []byte(order.OrderID),
		Value: value,
	}); err != nil {
		return fmt.Errorf("kafka: publish OrderCreated: %w", err)
	}
	return nil
}
