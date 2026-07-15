package kafka

import (
	"context"
	"errors"
	"fmt"
	"log"

	eventsv1 "orderproc/proto/gen/events/v1"
	inventoryv1 "orderproc/proto/gen/inventory/v1"
	"orderproc/services/order/internal/store"

	segmentio "github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

const (
	TopicPaymentCompleted = "payment-completed"
	TopicPaymentFailed    = "payment-failed"
	consumerGroupID       = "order-service"
)

// ResultConsumer updates order status from payment result events, compensating
// (releasing the Inventory reservation) when payment fails. Delivery is
// at-least-once: each loop below uses FetchMessage + CommitMessages (not the
// auto-committing ReadMessage) so a message is only committed after its handler
// succeeds. Kafka's per-partition commit is cumulative -- committing message N+1
// would silently also commit a failed, uncommitted message N -- so a handler
// error stops the whole loop rather than skipping forward. Recovery is a
// restart: a new reader resumes from the last committed offset and redelivers
// the failed message. A truly unparseable message (unmarshal failure, not a
// handler failure) is the one thing skipped-and-committed, since redelivering a
// corrupt message forever can't ever succeed.
type ResultConsumer struct {
	store     *store.Store
	invClient inventoryv1.InventoryServiceClient
}

func NewResultConsumer(s *store.Store, invClient inventoryv1.InventoryServiceClient) *ResultConsumer {
	return &ResultConsumer{store: s, invClient: invClient}
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
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}

		var evt eventsv1.PaymentCompleted
		if err := proto.Unmarshal(msg.Value, &evt); err != nil {
			log.Printf("payment-completed: unmarshal: %v", err)
			if err := reader.CommitMessages(ctx, msg); err != nil {
				return err
			}
			continue
		}

		if err := c.handlePaymentCompleted(ctx, &evt); err != nil {
			log.Printf("payment-completed: handle order %q: %v (not committed, will redeliver)", evt.GetOrderId(), err)
			return err
		}
		if err := reader.CommitMessages(ctx, msg); err != nil {
			return err
		}
	}
}

// handlePaymentCompleted confirms orderID, unless it's no longer PENDING. That
// covers both a redelivery of this same event (already CONFIRMED -- harmless
// no-op) and a conflicting PaymentFailed that already won the race (already
// CANCELLED, its reservation already released) -- overwriting the latter back
// to CONFIRMED would be a real bug, not just a missed dedup, since the
// reservation backing it is already gone.
func (c *ResultConsumer) handlePaymentCompleted(ctx context.Context, evt *eventsv1.PaymentCompleted) error {
	currentStatus, err := c.store.UpdateStatusIfPending(ctx, evt.GetOrderId(), "CONFIRMED")
	if err != nil {
		return err
	}
	if currentStatus != "CONFIRMED" {
		log.Printf("payment-completed: order %q already %s, not confirming", evt.GetOrderId(), currentStatus)
	}
	return nil
}

// RunPaymentFailed consumes payment-failed until ctx is cancelled.
func (c *ResultConsumer) RunPaymentFailed(ctx context.Context, brokers []string) error {
	reader := newReader(brokers, TopicPaymentFailed)
	defer reader.Close()

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}

		var evt eventsv1.PaymentFailed
		if err := proto.Unmarshal(msg.Value, &evt); err != nil {
			log.Printf("payment-failed: unmarshal: %v", err)
			if err := reader.CommitMessages(ctx, msg); err != nil {
				return err
			}
			continue
		}

		if err := c.handlePaymentFailed(ctx, &evt); err != nil {
			log.Printf("payment-failed: handle order %q: %v (not committed, will redeliver)", evt.GetOrderId(), err)
			return err
		}
		if err := reader.CommitMessages(ctx, msg); err != nil {
			return err
		}
	}
}

// handlePaymentFailed is the compensation path: release the Inventory
// reservation and cancel the order.
//
// The status transition is claimed FIRST, before touching Inventory at all --
// that ordering, not just deduping the redelivery, is what makes this safe:
//
//   - If currentStatus == "CONFIRMED": a PaymentCompleted already won the race
//     for this order -- this PaymentFailed is stale/conflicting. Release is
//     never called. The reservation backs a confirmed order; releasing it here
//     would double-book that stock while the confirmed order still expects it
//     filled.
//   - If currentStatus == "CANCELLED" (whether this call just set it, or a
//     prior delivery of this same PaymentFailed already did): Release is
//     called every time, not just the first. That's what makes this safe
//     against partial failure -- if Release errored on a previous delivery
//     (status already flipped, stock never actually released), returning that
//     error here leaves the message uncommitted, so it's redelivered, and the
//     redelivered call reads currentStatus == "CANCELLED" again and retries
//     Release (itself already idempotent -- a second call finds no
//     reservation rows and no-ops). Gating Release on "did the UPDATE apply
//     just now" instead would silently drop that retry and leak the
//     reservation forever.
//
// Any other currentStatus is a data invariant violation (only PENDING,
// CONFIRMED, and CANCELLED are ever written) -- treated as a hard error
// rather than silently ignored, since committing past it would hide a real
// bug.
func (c *ResultConsumer) handlePaymentFailed(ctx context.Context, evt *eventsv1.PaymentFailed) error {
	orderID := evt.GetOrderId()

	currentStatus, err := c.store.UpdateStatusIfPending(ctx, orderID, "CANCELLED")
	if err != nil {
		return err
	}

	switch currentStatus {
	case "CONFIRMED":
		log.Printf("payment-failed: order %q already CONFIRMED, not releasing (stale/conflicting event)", orderID)
		return nil
	case "CANCELLED":
		if _, err := c.invClient.Release(ctx, &inventoryv1.ReleaseRequest{OrderId: orderID}); err != nil {
			return fmt.Errorf("release reservation for order %q: %w", orderID, err)
		}
		return nil
	default:
		return fmt.Errorf("payment-failed: order %q has unexpected status %q", orderID, currentStatus)
	}
}
