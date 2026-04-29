SELECT 'olist_orders' AS table_name, COUNT(*) AS duplicate_count 
FROM (SELECT order_id FROM olist_orders_dataset GROUP BY order_id HAVING COUNT(*) > 1) t
UNION ALL
SELECT 'olist_order_items', COUNT(*) 
FROM (SELECT order_id, order_item_id FROM olist_order_items_dataset GROUP BY order_id, order_item_id HAVING COUNT(*) > 1) t
UNION ALL
SELECT 'olist_order_payments', COUNT(*) 
FROM (SELECT order_id, payment_sequential FROM olist_order_payments_dataset GROUP BY order_id, payment_sequential HAVING COUNT(*) > 1) t
UNION ALL
SELECT 'olist_order_reviews', COUNT(*) 
FROM (SELECT review_id FROM olist_order_reviews_dataset GROUP BY review_id HAVING COUNT(*) > 1) t
-- same review id for different orders, problaly to olis logistics dealing with new partners, the pk shoud be a compost key beteen review and orders id
UNION ALL
SELECT 'olist_customers', COUNT(*) 
FROM (SELECT customer_id FROM olist_customers_dataset GROUP BY customer_id HAVING COUNT(*) > 1) t
UNION ALL
SELECT 'olist_products', COUNT(*) 
FROM (SELECT product_id FROM olist_products_dataset GROUP BY product_id HAVING COUNT(*) > 1) t
UNION ALL
SELECT 'olist_sellers', COUNT(*) 
FROM (SELECT seller_id FROM olist_sellers_dataset GROUP BY seller_id HAVING COUNT(*) > 1) t
UNION ALL
SELECT 'product_category_translation', COUNT(*) 
FROM (SELECT product_category_name FROM product_category_name_translation GROUP BY product_category_name HAVING COUNT(*) > 1) t
UNION ALL
SELECT 'olist_geolocation', COUNT(*) -- avoid multiplication of results, due to more than 1 long and lat for the same cep
FROM (SELECT geolocation_zip_code_prefix FROM olist_geolocation_dataset GROUP BY geolocation_zip_code_prefix HAVING COUNT(*) > 1) t;