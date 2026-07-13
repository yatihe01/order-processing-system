// Package server implements the InventoryService gRPC API on top of the store.
package server

import (
	"context"
	"fmt"

	inventoryv1 "orderproc/proto/gen/inventory/v1"
	"orderproc/services/inventory/internal/store"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type InventoryServer struct {
	inventoryv1.UnimplementedInventoryServiceServer
	store *store.Store
}

func New(s *store.Store) *InventoryServer {
	return &InventoryServer{store: s}
}

func (s *InventoryServer) Reserve(ctx context.Context, req *inventoryv1.ReserveRequest) (*inventoryv1.ReserveResponse, error) {
	if req.GetOrderId() == "" {
		return nil, status.Error(codes.InvalidArgument, "order_id is required")
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

	ok, reason, err := s.store.Reserve(ctx, req.GetOrderId(), items)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Errorf("reserve: %w", err).Error())
	}

	return &inventoryv1.ReserveResponse{Success: ok, Reason: reason}, nil
}

func (s *InventoryServer) Release(ctx context.Context, req *inventoryv1.ReleaseRequest) (*inventoryv1.ReleaseResponse, error) {
	if req.GetOrderId() == "" {
		return nil, status.Error(codes.InvalidArgument, "order_id is required")
	}

	if err := s.store.Release(ctx, req.GetOrderId()); err != nil {
		return nil, status.Error(codes.Internal, fmt.Errorf("release: %w", err).Error())
	}

	return &inventoryv1.ReleaseResponse{Success: true}, nil
}
