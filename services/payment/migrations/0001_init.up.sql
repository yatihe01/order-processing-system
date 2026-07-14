-- order_id is the PK (not payment_id): Payment processes at most one charge per
-- order, and it's the natural lookup key a future idempotency check (Phase 4:
-- "has this order already been charged?") would use.
CREATE TABLE payments (
  order_id   CHAR(26)    NOT NULL,
  payment_id CHAR(26)    NOT NULL,
  status     VARCHAR(20) NOT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (order_id)
) ENGINE=InnoDB;
