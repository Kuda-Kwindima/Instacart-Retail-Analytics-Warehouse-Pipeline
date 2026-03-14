CREATE SCHEMA IF NOT EXISTS warehouse;

DROP TABLE IF EXISTS warehouse.dim_aisles;

CREATE TABLE warehouse.dim_aisles AS
SELECT DISTINCT
    aisle_id,
    aisle
FROM raw.raw_aisles;

SELECT COUNT(*) FROM warehouse.dim_aisles;
