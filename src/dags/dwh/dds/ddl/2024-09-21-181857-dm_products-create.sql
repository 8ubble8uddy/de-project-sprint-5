CREATE TABLE IF NOT EXISTS dds.dm_products (
	id INT NOT NULL GENERATED ALWAYS AS IDENTITY,
	restaurant_id INT NOT NULL,
	product_id VARCHAR NOT NULL,
	product_name VARCHAR NOT NULL,
	product_price NUMERIC(14,2) NOT NULL DEFAULT 0,
	active_from TIMESTAMP NOT NULL,
	active_to TIMESTAMP NOT NULL,

    CONSTRAINT dm_products_pkey PRIMARY KEY(id),
    CONSTRAINT dm_products_product_price_check CHECK(product_price BETWEEN 0 AND 999999999999.99),
    CONSTRAINT dm_products_restaurant_id_fkey FOREIGN KEY(restaurant_id) REFERENCES dds.dm_restaurants,
    CONSTRAINT dm_products_product_id_active_to_uindex UNIQUE(product_id, active_to)
);
