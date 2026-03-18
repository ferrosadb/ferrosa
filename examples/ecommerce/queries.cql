USE store;

-- Browse products by category
SELECT name, price, in_stock
FROM products_by_category
WHERE category = 'electronics';

-- View a product detail page
SELECT * FROM products
WHERE product_id = 11111111-1111-1111-1111-111111111111;

-- View the shopping cart
SELECT product_id, quantity, added_at
FROM shopping_cart
WHERE user_id = aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa;

-- View order history
SELECT order_id, status, total, items
FROM orders
WHERE user_id = aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
LIMIT 10;

-- Check if a product is in stock
SELECT in_stock FROM products
WHERE product_id = 55555555-5555-5555-5555-555555555555;

-- View tags for a product
SELECT name, tags FROM products
WHERE product_id = 11111111-1111-1111-1111-111111111111;

-- View attributes for a product
SELECT name, attributes FROM products
WHERE product_id = 22222222-2222-2222-2222-222222222222;

-- View order items
SELECT order_id, items FROM orders
WHERE user_id = aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
LIMIT 1;
