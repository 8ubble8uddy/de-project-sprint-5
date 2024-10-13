CREATE TABLE IF NOT EXISTS stg.bonussystem_ranks (
	id 	INT NOT NULL,
	"name" VARCHAR(2048) NOT NULL,
	bonus_percent NUMERIC(19,5) DEFAULT 0 NOT NULL,
	min_payment_threshold NUMERIC(19,5) DEFAULT 0 NOT NULL,

	CONSTRAINT bonussystem_ranks_pkey PRIMARY KEY(id)
);
