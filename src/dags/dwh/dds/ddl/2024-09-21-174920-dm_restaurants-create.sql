CREATE TABLE IF NOT EXISTS dds.dm_restaurants (
	id INT NOT NULL GENERATED ALWAYS AS IDENTITY,
	restaurant_id VARCHAR NOT NULL,
	restaurant_name VARCHAR NOT NULL,
	active_from TIMESTAMP NOT NULL,
	active_to TIMESTAMP NOT NULL,

    CONSTRAINT dm_restaurants_pkey PRIMARY KEY(id),
    CONSTRAINT dm_restaurants_restaurant_id_active_to_uindex UNIQUE(restaurant_id, active_to)
);
