--Combine streaming data into a denormalized curated table for analysis
CREATE OR REPLACE TABLE ecommerce.curated_orders_with_inventory AS
SELECT
  o.order_id,
  o.user_id,
  o.product_id,
  o.quantity,
  o.price,
  i.warehouse_id,
  i.quantity_in_stock,
  TIMESTAMP_SECONDS(CAST(o.timestamp AS INT64)) AS order_ts
FROM `ecommerce.orders_stream` o
LEFT JOIN `ecommerce.inventory_stream` i
ON o.product_id = i.product_id;