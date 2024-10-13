CREATE TABLE IF NOT EXISTS stg.deliverysystem_restaurants (
    id SERIAL NOT NULL,
    object_id VARCHAR(2048) NOT NULL,
    "name" TEXT NOT NULL,

	CONSTRAINT deliverysystem_restaurants_pkey PRIMARY KEY(id),
    CONSTRAINT deliverysystem_restaurants_object_id_uindex UNIQUE(object_id)
);
