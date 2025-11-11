CREATE OR REPLACE TABLE ecommerce.curated_orders_with_inventory AS
SELECT
  o.order_id,
  o.user_id,
  o.product_id,
  o.quantity,
  o.price,
  i.warehouse_id,
  i.quantity_in_stock,
  o.order_date
FROM `ecommerce.orders_batch` o
LEFT JOIN (
  SELECT product_id, warehouse_id, quantity_in_stock
  FROM `ecommerce.inventory_stream`
  QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_updated DESC) = 1
) i
USING(product_id);