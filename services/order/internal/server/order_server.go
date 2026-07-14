// Package server implements the OrderService gRPC API on top of the store.
package server

import (
	"context"
	"errors"
	"fmt"
	"time"

	inventoryv1 "orderproc/proto/gen/inventory/v1"
	orderv1 "orderproc/proto/gen/order/v1"
	"orderproc/services/order/internal/kafka"
	"orderproc/services/order/internal/store"

	"github.com/oklog/ulid/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// reserveTimeout bounds how long CreateOrder waits on the Inventory reservation call.
// A full retry/timeout policy is Decision #6 (deferred); this is just a sane bound with
// no retries yet.
const reserveTimeout = 3 * time.Second

type OrderServer struct {
	orderv1.UnimplementedOrderServiceServer
	store     *store.Store
	invClient inventoryv1.InventoryServiceClient
	producer  *kafka.Producer
}

func New(s *store.Store, invClient inventoryv1.InventoryServiceClient, producer *kafka.Producer) *OrderServer {
	return &OrderServer{store: s, invClient: invClient, producer: producer}
}

func (s *OrderServer) CreateOrder(ctx context.Context, req *orderv1.CreateOrderRequest) (*orderv1.CreateOrderResponse, error) {
	if req.GetUserId() == "" {
		return nil, status.Error(codes.InvalidArgument, "user_id is required")
	}
	if len(req.GetItems()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one item is required")
	}

	items := make([]store.Item, 0, len(req.GetItems()))
	for _, i := range req.GetItems() {
		if i.GetProductId() == "" {
			return nil, status.Error(codes.InvalidArgument, "item product_id is required")
		}
		if i.GetQuantity() <= 0 {
			return nil, status.Errorf(codes.InvalidArgument, "item %q quantity must be > 0", i.GetProductId())
		}
		items = append(items, store.Item{ProductID: i.GetProductId(), Quantity: i.GetQuantity()})
	}

	orderID := ulid.Make().String()

	reserveCtx, cancel := context.WithTimeout(ctx, reserveTimeout)
	defer cancel()
	reserveResp, err := s.invClient.Reserve(reserveCtx, &inventoryv1.ReserveRequest{
		OrderId: orderID,
		Items:   toReservationItems(items),
	})
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Errorf("reserve inventory: %w", err).Error())
	}
	if !reserveResp.GetSuccess() {
		return nil, status.Errorf(codes.FailedPrecondition, "reservation failed: %s", reserveResp.GetReason())
	}

	order, err := s.store.CreateOrder(ctx, orderID, req.GetUserId(), items)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Errorf("create order: %w", err).Error())
	}

	// Known gap: this write and the DB commit above aren't atomic (the dual-write
	// problem). If this publish fails, the order row exists but no event ever fires
	// to trigger payment. The fix is a transactional outbox; not built here -- see
	// CLAUDE.md / ROADMAP Phase 3 notes.
	if err := s.producer.PublishOrderCreated(ctx, order); err != nil {
		return nil, status.Error(codes.Internal, fmt.Errorf("publish order created: %w", err).Error())
	}

	return &orderv1.CreateOrderResponse{
		OrderId: order.OrderID,
		Status:  statusToProto(order.Status),
	}, nil
}

func toReservationItems(items []store.Item) []*inventoryv1.ReservationItem {
	out := make([]*inventoryv1.ReservationItem, 0, len(items))
	for _, i := range items {
		out = append(out, &inventoryv1.ReservationItem{ProductId: i.ProductID, Quantity: i.Quantity})
	}
	return out
}

func (s *OrderServer) GetOrder(ctx context.Context, req *orderv1.GetOrderRequest) (*orderv1.GetOrderResponse, error) {
	if req.GetOrderId() == "" {
		return nil, status.Error(codes.InvalidArgument, "order_id is required")
	}

	order, err := s.store.GetOrder(ctx, req.GetOrderId())
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return nil, status.Errorf(codes.NotFound, "order %q not found", req.GetOrderId())
		}
		return nil, status.Error(codes.Internal, fmt.Errorf("get order: %w", err).Error())
	}

	items := make([]*orderv1.OrderItem, 0, len(order.Items))
	for _, i := range order.Items {
		items = append(items, &orderv1.OrderItem{ProductId: i.ProductID, Quantity: i.Quantity})
	}

	return &orderv1.GetOrderResponse{
		Order: &orderv1.Order{
			OrderId:   order.OrderID,
			UserId:    order.UserID,
			Items:     items,
			Status:    statusToProto(order.Status),
			CreatedAt: order.CreatedAt.Unix(),
		},
	}, nil
}

// statusToProto maps the persisted status string to the proto enum. Unrecognized
// values map to UNKNOWN rather than panicking -- the DB is the source of truth,
// not the enum, so a mismatch here is a bug to surface, not to crash on.
func statusToProto(s string) orderv1.OrderStatus {
	switch s {
	case "PENDING":
		return orderv1.OrderStatus_ORDER_STATUS_PENDING
	case "CONFIRMED":
		return orderv1.OrderStatus_ORDER_STATUS_CONFIRMED
	case "CANCELLED":
		return orderv1.OrderStatus_ORDER_STATUS_CANCELLED
	case "FAILED":
		return orderv1.OrderStatus_ORDER_STATUS_FAILED
	default:
		return orderv1.OrderStatus_ORDER_STATUS_UNKNOWN
	}
}
