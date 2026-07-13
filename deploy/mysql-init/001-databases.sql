-- One database per service, sharing a single MySQL instance for local dev.
-- Physically separate instances (or at least separate credentials) would be
-- the production shape; noted as a "what I'd do differently at scale" item.
CREATE DATABASE IF NOT EXISTS order_db;
CREATE DATABASE IF NOT EXISTS inventory_db;
CREATE DATABASE IF NOT EXISTS payment_db;
