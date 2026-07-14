// Package charge implements a mock payment charge. There is no real payment
// gateway (explicitly out of scope per CLAUDE.md) -- this is a deterministic
// stand-in so both outcomes are reproducible in tests/demos, and the seam
// where a real gateway integration would go later.
package charge

import orderv1 "orderproc/proto/gen/order/v1"

// declineProductID is a magic value: any order containing it is declined.
const declineProductID = "sku-decline"

// Run returns ok=false with a reason if the charge is declined.
func Run(items []*orderv1.OrderItem) (ok bool, reason string) {
	for _, item := range items {
		if item.GetProductId() == declineProductID {
			return false, "card declined"
		}
	}
	return true, ""
}
