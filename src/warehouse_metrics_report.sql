SELECT
  warehouse_id,
  COUNT(order_id) AS total_orders,
  SUM(price * quantity) AS total_revenue,
  SUM(quantity) AS total_quantity_ordered,
  AVG(IFNULL(quantity_in_stock, 0)) AS avg_stock,
  COUNTIF(quantity_in_stock < quantity) AS out_of_stock_count
FROM ecommerce.curated_orders_with_inventory
GROUP BY warehouse_id
ORDER BY total_revenue DESC;