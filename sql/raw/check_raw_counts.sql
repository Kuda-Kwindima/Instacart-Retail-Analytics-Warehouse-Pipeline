SELECT COUNT(*) FROM raw.raw_orders;

SELECT 'raw_orders' AS table_name, COUNT(*) AS row_count FROM raw.raw_orders
UNION ALL
SELECT 'raw_products', COUNT(*) FROM raw.raw_products
UNION ALL
SELECT 'raw_aisles', COUNT(*) FROM raw.raw_aisles
UNION ALL
SELECT 'raw_departments', COUNT(*) FROM raw.raw_departments
UNION ALL
SELECT 'raw_order_products_prior', COUNT(*) FROM raw.raw_order_products_prior
UNION ALL
SELECT 'raw_order_products_train', COUNT(*) FROM raw.raw_order_products_train;
