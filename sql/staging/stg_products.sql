CREATE SCHEMA IF NOT EXISTS staging;

DROP TABLE IF EXISTS staging.stg_products;

CREATE TABLE staging.stg_products AS
SELECT
    product_id,
    product_name,
    aisle_id,
    department_id
FROM raw.raw_products;

SELECT COUNT(*) FROM staging.stg_products;