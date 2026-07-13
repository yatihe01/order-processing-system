CREATE TABLE inventory (
  product_id VARCHAR(64)  NOT NULL,
  quantity   INT UNSIGNED NOT NULL,
  PRIMARY KEY (product_id)
) ENGINE=InnoDB;

-- Release(order_id) in the proto carries no items, so Inventory must remember what it
-- reserved per order to know what to restore. Also lays groundwork for Decision #4
-- (idempotency): checking for an existing reservation before re-applying a redelivered
-- Reserve call is a Phase 4 concern, deliberately not implemented yet.
CREATE TABLE reservations (
  order_id   CHAR(26)     NOT NULL,   -- matches Order service's ULID order_id
  product_id VARCHAR(64)  NOT NULL,
  quantity   INT UNSIGNED NOT NULL,
  created_at DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (order_id, product_id),
  CONSTRAINT fk_reservations_inventory
    FOREIGN KEY (product_id) REFERENCES inventory(product_id)
) ENGINE=InnoDB;
