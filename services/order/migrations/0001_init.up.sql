CREATE TABLE orders (
  order_id   CHAR(26)     NOT NULL,            -- ULID
  user_id    VARCHAR(64)  NOT NULL,
  status     VARCHAR(20)  NOT NULL DEFAULT 'PENDING',
  created_at DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (order_id)
) ENGINE=InnoDB;

CREATE TABLE order_items (
  order_id   CHAR(26)     NOT NULL,
  product_id VARCHAR(64)  NOT NULL,
  quantity   INT UNSIGNED NOT NULL,
  PRIMARY KEY (order_id, product_id),
  CONSTRAINT fk_order_items_order
    FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE,
  CONSTRAINT chk_order_items_quantity CHECK (quantity > 0)
) ENGINE=InnoDB;
