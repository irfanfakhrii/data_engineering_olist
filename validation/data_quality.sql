SELECT COUNT(*) FROM dim_customers;
SELECT COUNT(*) FROM fact_order_items;

SELECT COUNT(*)FROM fact_order_items
WHERE price IS NULL;

SELECT COUNT(*)FROM fact_order_items f
LEFT JOIN dim_customers c
ON f.customer_id = c.customer_id
WHERE c.customer_id IS NULL;