--Combine streaming data into a denormalized curated table for analysis
CREATE OR REPLACE TABLE ecommerce.curated_orders_with_inventory AS
SELECT
  eos.order_id,
  eos.user_id,
  eos.product_id,
  eos.quantity,
  eos.price,
  eis.warehouse_id,
  eis.quantity_in_stock,
  TIMESTAMP_SECONDS(CAST(eos.timestamp AS INT64)) AS order_ts
FROM `ecommerce.orders_stream` eos
LEFT JOIN `ecommerce.inventory_stream` eis
ON eos.product_id = eis.product_id;