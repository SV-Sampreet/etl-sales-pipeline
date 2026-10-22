-- Sales ETL Pipeline Database Schema
-- This schema defines the orders table for storing cleaned sales data

-- Create orders table if not exists
CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    order_date DATE NOT NULL,
    product TEXT NOT NULL,
    category TEXT NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price REAL NOT NULL CHECK (unit_price >= 0),
    region TEXT NOT NULL,
    revenue REAL NOT NULL CHECK (revenue >= 0),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes if not exists (SQLite doesn't support IF NOT EXISTS for indexes, so we ignore errors)
CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_order_date ON orders(order_date);
CREATE INDEX IF NOT EXISTS idx_orders_category ON orders(category);
CREATE INDEX IF NOT EXISTS idx_orders_region ON orders(region);
