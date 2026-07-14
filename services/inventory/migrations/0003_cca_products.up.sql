-- Add product display name and price to inventory.
-- name is explicitly utf8mb4 so it can hold Chinese characters if needed later.
-- price_cents stores money as a whole number of cents (never a decimal/float),
-- so totals never hit floating-point rounding errors. $8.00 -> 800.
ALTER TABLE inventory
  ADD COLUMN name        VARCHAR(128) CHARACTER SET utf8mb4 NOT NULL DEFAULT '',
  ADD COLUMN price_cents INT UNSIGNED NOT NULL DEFAULT 0;
 
-- Seed the CCA store's real products.
-- Existing sku-1 / sku-2 rows from 0002_seed are left in place (the integration
-- tests still reference them); these five are the real merch.
INSERT INTO inventory (product_id, quantity, name, price_cents) VALUES
  ('cable-serious',       40, 'cable1 - serious design',       800),
  ('cable-cute',          40, 'cable2 - cute design',          800),
  ('cardholder-serious',  60, 'card holder1 - serious design', 500),
  ('cardholder-cute',     60, 'card holder2 - cute design',    500),
  ('team-badge',         100, 'team badge',                   2000);