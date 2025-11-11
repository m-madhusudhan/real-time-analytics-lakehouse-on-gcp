SELECT *
FROM 'qwiklabs-gcp-01-cffc452c1796.ecommerce.orders_stream'
LIMIT 10;

SELECT *
FROM 'qwiklabs-gcp-01-cffc452c1796.ecommerce.inventory_stream'
LIMIT 10;

SELECT product_id, SUM(quantity) AS total_ordered_quantity
FROM 'qwiklabs-gcp-01-cffc452c1796.ecommerce.orders_stream'
GROUP BY product_id
ORDER BY total_ordered_quantity DESC;

SELECT product_id, SUM(quantity_in_stock) AS total_stock
FROM 'qwiklabs-gcp-01-cffc452c1796.ecommerce.inventory_stream'
GROUP BY product_id
ORDER BY total_stock DESC;


SELECT 
    product_id,
    SUM(quantity) AS total_ordered_quantity,
    SUM(quantity * price) AS total_revenue
FROM 'qwiklabs-gcp-01-cffc452c1796.ecommerce.orders_stream'
GROUP BY product_id
ORDER BY total_revenue DESC;