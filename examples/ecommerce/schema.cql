CREATE KEYSPACE IF NOT EXISTS store WITH replication = {
    'class': 'SimpleStrategy', 'replication_factor': 3
};

USE store;

CREATE TABLE IF NOT EXISTS products (
    product_id  uuid PRIMARY KEY,
    name        text,
    description text,
    category    text,
    price       decimal,
    currency    text,
    in_stock    boolean,
    tags        set<text>,
    attributes  map<text, text>,
    created_at  timestamp
);

CREATE TABLE IF NOT EXISTS products_by_category (
    category   text,
    product_id uuid,
    name       text,
    price      decimal,
    in_stock   boolean,
    PRIMARY KEY (category, name, product_id)
);

CREATE TABLE IF NOT EXISTS shopping_cart (
    user_id    uuid,
    product_id uuid,
    quantity   int,
    added_at   timestamp,
    PRIMARY KEY (user_id, product_id)
);

CREATE TABLE IF NOT EXISTS orders (
    user_id          uuid,
    order_id         timeuuid,
    status           text,
    total            decimal,
    items            list<frozen<tuple<uuid, text, int, decimal>>>,
    shipping_address text,
    PRIMARY KEY (user_id, order_id)
) WITH CLUSTERING ORDER BY (order_id DESC);
