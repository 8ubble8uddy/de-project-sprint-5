CREATE TABLE IF NOT EXISTS dds.dm_users (
	id INT NOT NULL GENERATED ALWAYS AS IDENTITY,
	user_id VARCHAR NOT NULL,
	user_name VARCHAR NOT NULL,
	user_login VARCHAR NOT NULL,

    CONSTRAINT dm_users_pkey PRIMARY KEY(id),
    CONSTRAINT dm_users_user_id_uindex UNIQUE(user_id)
);
