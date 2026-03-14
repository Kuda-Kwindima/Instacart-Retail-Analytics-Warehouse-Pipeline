CREATE SCHEMA IF NOT EXISTS warehouse;

DROP TABLE IF EXISTS warehouse.dim_departments;

CREATE TABLE warehouse.dim_departments AS
SELECT DISTINCT
    department_id,
    department
FROM raw.raw_departments;

SELECT COUNT(*) FROM warehouse.dim_departments;
