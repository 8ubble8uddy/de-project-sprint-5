CREATE TABLE IF NOT EXISTS stg.deliverysystem_deliveries(
    id SERIAL NOT NULL,
    delivery_id VARCHAR(2048) NOT NULL,
    delivery_ts TIMESTAMP NOT NULL,
    delivery_value TEXT NOT NULL,

	CONSTRAINT deliverysystem_deliveries_pkey PRIMARY KEY(id),
    CONSTRAINT deliverysystem_deliveries_delivery_id_uindex UNIQUE(delivery_id)
);
