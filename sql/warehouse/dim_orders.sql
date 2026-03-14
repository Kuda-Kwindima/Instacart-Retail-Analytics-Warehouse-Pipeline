CREATE SCHEMA IF NOT EXISTS warehouse;

DROP TABLE IF EXISTS warehouse.dim_orders;

CREATE TABLE warehouse.dim_orders AS
SELECT
    order_id,
    user_id,
    eval_set,
    order_number,
    order_dow,
    order_hour_of_day,
    days_since_prior_order
FROM staging.stg_orders;

SELECT COUNT(*) FROM warehouse.dim_orders;