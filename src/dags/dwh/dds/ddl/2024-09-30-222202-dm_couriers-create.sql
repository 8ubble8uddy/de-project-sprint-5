CREATE TABLE IF NOT EXISTS dds.dm_couriers(
	id INT NOT NULL GENERATED ALWAYS AS IDENTITY,
	courier_id VARCHAR NOT NULL,
	courier_name VARCHAR NOT NULL,

    CONSTRAINT dm_couriers_pkey PRIMARY KEY(id),
    CONSTRAINT dm_couriers_courier_id_uindex UNIQUE(courier_id)
);
