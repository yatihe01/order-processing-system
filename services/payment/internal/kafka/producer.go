// Package kafka consumes OrderCreated and publishes payment result events.
package kafka

import (
	"context"
	"fmt"
	"time"

	eventsv1 "orderproc/proto/gen/events/v1"

	"github.com/oklog/ulid/v2"
	segmentio "github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

const (
	TopicOrderCreated     = "order-created"
	TopicPaymentCompleted = "payment-completed"
	TopicPaymentFailed    = "payment-failed"
)

// Producer publishes payment result events, keyed by order_id (Decision #5).
type Producer struct {
	completed *segmentio.Writer
	failed    *segmentio.Writer
}

func NewProducer(brokers []string) *Producer {
	newWriter := func(topic string) *segmentio.Writer {
		return &segmentio.Writer{
			Addr:     segmentio.TCP(brokers...),
			Topic:    topic,
			Balancer: &segmentio.Hash{},
		}
	}
	return &Producer{
		completed: newWriter(TopicPaymentCompleted),
		failed:    newWriter(TopicPaymentFailed),
	}
}

func (p *Producer) Close() error {
	err1 := p.completed.Close()
	err2 := p.failed.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

func (p *Producer) PublishPaymentCompleted(ctx context.Context, orderID, paymentID string) error {
	evt := &eventsv1.PaymentCompleted{
		EventId:   ulid.Make().String(),
		OrderId:   orderID,
		PaymentId: paymentID,
		Timestamp: time.Now().Unix(),
	}
	value, err := proto.Marshal(evt)
	if err != nil {
		return fmt.Errorf("kafka: marshal PaymentCompleted: %w", err)
	}
	if err := p.completed.WriteMessages(ctx, segmentio.Message{Key: []byte(orderID), Value: value}); err != nil {
		return fmt.Errorf("kafka: publish PaymentCompleted: %w", err)
	}
	return nil
}

func (p *Producer) PublishPaymentFailed(ctx context.Context, orderID, reason string) error {
	evt := &eventsv1.PaymentFailed{
		EventId:   ulid.Make().String(),
		OrderId:   orderID,
		Reason:    reason,
		Timestamp: time.Now().Unix(),
	}
	value, err := proto.Marshal(evt)
	if err != nil {
		return fmt.Errorf("kafka: marshal PaymentFailed: %w", err)
	}
	if err := p.failed.WriteMessages(ctx, segmentio.Message{Key: []byte(orderID), Value: value}); err != nil {
		return fmt.Errorf("kafka: publish PaymentFailed: %w", err)
	}
	return nil
}
