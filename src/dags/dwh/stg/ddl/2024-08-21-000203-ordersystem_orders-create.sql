CREATE TABLE IF NOT EXISTS stg.ordersystem_orders (
    id SERIAL NOT NULL,
    object_id VARCHAR(2048) NOT NULL,
    object_value TEXT NOT NULL,
    update_ts TIMESTAMP NOT NULL,

	CONSTRAINT ordersystem_orders_pkey PRIMARY KEY(id),
    CONSTRAINT ordersystem_orders_object_id_uindex UNIQUE(object_id)
);
