CREATE TABLE IF NOT EXISTS stg.deliverysystem_couriers (
    id SERIAL NOT NULL,
    object_id VARCHAR(2048) NOT NULL,
    "name" TEXT NOT NULL,

	CONSTRAINT deliverysystem_couriers_pkey PRIMARY KEY(id),
    CONSTRAINT deliverysystem_couriers_object_id_uindex UNIQUE(object_id)
);
