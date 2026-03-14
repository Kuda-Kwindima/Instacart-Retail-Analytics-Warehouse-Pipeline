CREATE SCHEMA IF NOT EXISTS staging;

DROP TABLE IF EXISTS staging.stg_order_items;

CREATE TABLE staging.stg_order_items AS
SELECT
    order_id,
    product_id,
    add_to_cart_order,
    reordered,
    'prior' AS source_set
FROM raw.raw_order_products_prior

UNION ALL

SELECT
    order_id,
    product_id,
    add_to_cart_order,
    reordered,
    'train' AS source_set
FROM raw.raw_order_products_train;

SELECT COUNT(*) FROM staging.stg_order_items;