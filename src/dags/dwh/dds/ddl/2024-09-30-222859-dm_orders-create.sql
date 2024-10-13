CREATE TABLE IF NOT EXISTS dds.dm_orders (
	id INT NOT NULL GENERATED ALWAYS AS IDENTITY,
	order_key VARCHAR NOT NULL,
	order_status VARCHAR NOT NULL,
	restaurant_id INT NOT NULL,
	timestamp_id INT NOT NULL,
	user_id INT NOT NULL,

    CONSTRAINT dm_orders_pkey PRIMARY KEY(id),
	CONSTRAINT dm_orders_order_key_uindex UNIQUE(order_key),
    CONSTRAINT dm_orders_user_id_fkey FOREIGN KEY(user_id) REFERENCES dds.dm_users,
    CONSTRAINT dm_orders_restaurant_id_fkey FOREIGN KEY(restaurant_id) REFERENCES dds.dm_restaurants,
    CONSTRAINT dm_orders_timestamp_id_fkey FOREIGN KEY(timestamp_id) REFERENCES dds.dm_timestamps
);
 