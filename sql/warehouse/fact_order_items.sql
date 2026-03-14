CREATE SCHEMA IF NOT EXISTS warehouse;

DROP TABLE IF EXISTS warehouse.fact_order_items;

CREATE TABLE warehouse.fact_order_items AS
SELECT
    order_id,
    product_id,
    add_to_cart_order,
    reordered,
    source_set
FROM staging.stg_order_items;

SELECT COUNT(*) FROM warehouse.fact_order_items;

CREATE INDEX idx_fact_order_items_product_id
ON warehouse.fact_order_items (product_id);

CREATE INDEX idx_fact_order_items_order_id
ON warehouse.fact_order_items (order_id);

CREATE INDEX idx_dim_products_product_id
ON warehouse.dim_products (product_id);

CREATE INDEX idx_dim_orders_order_id
ON warehouse.dim_orders (order_id);

