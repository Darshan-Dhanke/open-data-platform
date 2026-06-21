-- SILVER: project typed columns from the generic bronze CDC log and reduce to
-- latest-state-per-key (drop deletes, keep the newest change per primary key).
CREATE SCHEMA IF NOT EXISTS iceberg.silver WITH (location = 's3a://warehouse/iceberg/silver');

CREATE OR REPLACE TABLE iceberg.silver.customers AS
SELECT customer_id, name, email, country, segment, CAST(signup_date AS TIMESTAMP) AS signup_date
FROM (
  SELECT CAST(json_extract_scalar(after,'$.customer_id') AS BIGINT) customer_id,
         json_extract_scalar(after,'$.name') name,
         json_extract_scalar(after,'$.email') email,
         json_extract_scalar(after,'$.country') country,
         json_extract_scalar(after,'$.segment') segment,
         json_extract_scalar(after,'$.signup_date') signup_date,
         row_number() OVER (PARTITION BY json_extract_scalar(after,'$.customer_id') ORDER BY ts_ms DESC) rn
  FROM iceberg.bronze.cdc_events WHERE table_name='customers' AND op <> 'd'
) WHERE rn = 1;

CREATE OR REPLACE TABLE iceberg.silver.products AS
SELECT product_id, name, category, unit_price, unit_cost
FROM (
  SELECT CAST(json_extract_scalar(after,'$.product_id') AS BIGINT) product_id,
         json_extract_scalar(after,'$.name') name,
         json_extract_scalar(after,'$.category') category,
         CAST(json_extract_scalar(after,'$.unit_price') AS DOUBLE) unit_price,
         CAST(json_extract_scalar(after,'$.unit_cost') AS DOUBLE) unit_cost,
         row_number() OVER (PARTITION BY json_extract_scalar(after,'$.product_id') ORDER BY ts_ms DESC) rn
  FROM iceberg.bronze.cdc_events WHERE table_name='products' AND op <> 'd'
) WHERE rn = 1;

CREATE OR REPLACE TABLE iceberg.silver.orders AS
SELECT order_id, customer_id, CAST(order_ts AS TIMESTAMP) order_ts, channel, status
FROM (
  SELECT CAST(json_extract_scalar(after,'$.order_id') AS BIGINT) order_id,
         CAST(json_extract_scalar(after,'$.customer_id') AS BIGINT) customer_id,
         json_extract_scalar(after,'$.order_ts') order_ts,
         json_extract_scalar(after,'$.channel') channel,
         json_extract_scalar(after,'$.status') status,
         row_number() OVER (PARTITION BY json_extract_scalar(after,'$.order_id') ORDER BY ts_ms DESC) rn
  FROM iceberg.bronze.cdc_events WHERE table_name='orders' AND op <> 'd'
) WHERE rn = 1;

CREATE OR REPLACE TABLE iceberg.silver.order_items AS
SELECT item_id, order_id, product_id, quantity, unit_price
FROM (
  SELECT CAST(json_extract_scalar(after,'$.item_id') AS BIGINT) item_id,
         CAST(json_extract_scalar(after,'$.order_id') AS BIGINT) order_id,
         CAST(json_extract_scalar(after,'$.product_id') AS BIGINT) product_id,
         CAST(json_extract_scalar(after,'$.quantity') AS BIGINT) quantity,
         CAST(json_extract_scalar(after,'$.unit_price') AS DOUBLE) unit_price,
         row_number() OVER (PARTITION BY json_extract_scalar(after,'$.item_id') ORDER BY ts_ms DESC) rn
  FROM iceberg.bronze.cdc_events WHERE table_name='order_items' AND op <> 'd'
) WHERE rn = 1;

CREATE OR REPLACE TABLE iceberg.silver.payments AS
SELECT order_id, method, amount, status, CAST(paid_ts AS TIMESTAMP) paid_ts
FROM (
  SELECT CAST(json_extract_scalar(after,'$.order_id') AS BIGINT) order_id,
         json_extract_scalar(after,'$.method') method,
         CAST(json_extract_scalar(after,'$.amount') AS DOUBLE) amount,
         json_extract_scalar(after,'$.status') status,
         json_extract_scalar(after,'$.paid_ts') paid_ts,
         row_number() OVER (PARTITION BY json_extract_scalar(after,'$.order_id') ORDER BY ts_ms DESC) rn
  FROM iceberg.bronze.cdc_events WHERE table_name='payments' AND op <> 'd'
) WHERE rn = 1;
