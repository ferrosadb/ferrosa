USE store;

-- Wireless headphones
INSERT INTO products (product_id, name, description, category, price, currency, in_stock, tags, attributes, created_at)
VALUES (
    11111111-1111-1111-1111-111111111111,
    'ProSound Wireless Headphones',
    'Premium over-ear headphones with active noise cancellation and 30-hour battery life.',
    'electronics',
    149.99,
    'USD',
    true,
    {'wireless', 'bluetooth', 'noise-cancelling', 'over-ear'},
    {'brand': 'ProSound', 'color': 'matte black', 'battery_hours': '30', 'driver_size': '40mm'},
    '2024-01-10 09:00:00'
);

-- Mechanical keyboard
INSERT INTO products (product_id, name, description, category, price, currency, in_stock, tags, attributes, created_at)
VALUES (
    22222222-2222-2222-2222-222222222222,
    'TypeMaster Mechanical Keyboard',
    'Full-size mechanical keyboard with Cherry MX Brown switches and RGB backlighting.',
    'electronics',
    89.99,
    'USD',
    true,
    {'mechanical', 'rgb', 'full-size', 'usb-c'},
    {'brand': 'TypeMaster', 'switch_type': 'Cherry MX Brown', 'layout': 'US ANSI', 'connection': 'USB-C'},
    '2024-01-12 14:30:00'
);

-- Running shoes
INSERT INTO products (product_id, name, description, category, price, currency, in_stock, tags, attributes, created_at)
VALUES (
    33333333-3333-3333-3333-333333333333,
    'TrailBlazer Running Shoes',
    'Lightweight trail running shoes with responsive cushioning and aggressive grip.',
    'footwear',
    129.00,
    'USD',
    true,
    {'running', 'trail', 'lightweight', 'waterproof'},
    {'brand': 'TrailBlazer', 'size': '10', 'color': 'forest green', 'weight': '280g'},
    '2024-02-01 10:00:00'
);

-- USB-C Hub
INSERT INTO products (product_id, name, description, category, price, currency, in_stock, tags, attributes, created_at)
VALUES (
    44444444-4444-4444-4444-444444444444,
    'ConnectAll USB-C Hub',
    '7-in-1 USB-C hub with HDMI, USB-A, SD card reader, and 100W power delivery.',
    'electronics',
    59.99,
    'USD',
    true,
    {'usb-c', 'hub', 'hdmi', 'power-delivery'},
    {'brand': 'ConnectAll', 'ports': '7', 'power_delivery': '100W', 'color': 'space gray'},
    '2024-02-15 08:00:00'
);

-- Hiking boots
INSERT INTO products (product_id, name, description, category, price, currency, in_stock, tags, attributes, created_at)
VALUES (
    55555555-5555-5555-5555-555555555555,
    'Summit Hiking Boots',
    'Waterproof leather hiking boots with Vibram sole and ankle support.',
    'footwear',
    189.00,
    'USD',
    false,
    {'hiking', 'waterproof', 'leather', 'ankle-support'},
    {'brand': 'Summit', 'size': '10', 'color': 'brown', 'sole': 'Vibram'},
    '2024-03-01 11:00:00'
);

-- Products by category
INSERT INTO products_by_category (category, product_id, name, price, in_stock)
VALUES ('electronics', 44444444-4444-4444-4444-444444444444, 'ConnectAll USB-C Hub', 59.99, true);

INSERT INTO products_by_category (category, product_id, name, price, in_stock)
VALUES ('electronics', 11111111-1111-1111-1111-111111111111, 'ProSound Wireless Headphones', 149.99, true);

INSERT INTO products_by_category (category, product_id, name, price, in_stock)
VALUES ('electronics', 22222222-2222-2222-2222-222222222222, 'TypeMaster Mechanical Keyboard', 89.99, true);

INSERT INTO products_by_category (category, product_id, name, price, in_stock)
VALUES ('footwear', 55555555-5555-5555-5555-555555555555, 'Summit Hiking Boots', 189.00, false);

INSERT INTO products_by_category (category, product_id, name, price, in_stock)
VALUES ('footwear', 33333333-3333-3333-3333-333333333333, 'TrailBlazer Running Shoes', 129.00, true);

-- Shopping cart for customer
INSERT INTO shopping_cart (user_id, product_id, quantity, added_at)
VALUES (
    aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa,
    11111111-1111-1111-1111-111111111111,
    1,
    '2024-03-15 10:30:00'
);

INSERT INTO shopping_cart (user_id, product_id, quantity, added_at)
VALUES (
    aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa,
    22222222-2222-2222-2222-222222222222,
    1,
    '2024-03-15 10:32:00'
);

INSERT INTO shopping_cart (user_id, product_id, quantity, added_at)
VALUES (
    aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa,
    44444444-4444-4444-4444-444444444444,
    2,
    '2024-03-15 10:35:00'
);

-- Orders
INSERT INTO orders (user_id, order_id, status, total, items, shipping_address)
VALUES (
    aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa,
    d1b64ea0-d597-11ee-a506-000000000001,
    'delivered',
    258.00,
    [(33333333-3333-3333-3333-333333333333, 'TrailBlazer Running Shoes', 1, 129.00),
     (33333333-3333-3333-3333-333333333333, 'TrailBlazer Running Shoes', 1, 129.00)],
    '123 Main Street, Portland, OR 97201'
);

INSERT INTO orders (user_id, order_id, status, total, items, shipping_address)
VALUES (
    aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa,
    e2c75fb1-e698-11ee-b507-000000000002,
    'shipped',
    89.99,
    [(22222222-2222-2222-2222-222222222222, 'TypeMaster Mechanical Keyboard', 1, 89.99)],
    '123 Main Street, Portland, OR 97201'
);
