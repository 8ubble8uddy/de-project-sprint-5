CREATE TABLE IF NOT EXISTS stg.bonussystem_users (
	id INT NOT NULL,
	order_user_id TEXT NOT NULL,

	CONSTRAINT bonussystem_users_pkey PRIMARY KEY(id)
);
