CREATE SCHEMA IF NOT EXISTS staging;

DROP TABLE IF EXISTS staging.stg_orders;

CREATE TABLE staging.stg_orders AS
SELECT
    order_id,
    user_id,
    eval_set,
    order_number,
    order_dow,
    order_hour_of_day,
    days_since_prior_order
FROM raw.raw_orders;

SELECT COUNT(*) FROM staging.stg_orders;
