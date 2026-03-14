CREATE SCHEMA IF NOT EXISTS warehouse;

DROP TABLE IF EXISTS warehouse.dim_products;

CREATE TABLE warehouse.dim_products AS
SELECT
    p.product_id,
    p.product_name,
    p.aisle_id,
    a.aisle,
    p.department_id,
    d.department
FROM staging.stg_products p
LEFT JOIN warehouse.dim_aisles a
    ON p.aisle_id = a.aisle_id
LEFT JOIN warehouse.dim_departments d
    ON p.department_id = d.department_id;

SELECT COUNT(*) FROM warehouse.dim_products;
