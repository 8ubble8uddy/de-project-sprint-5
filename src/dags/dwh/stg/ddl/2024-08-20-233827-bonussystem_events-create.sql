CREATE TABLE IF NOT EXISTS stg.bonussystem_events (
	id INT NOT NULL,
	event_ts TIMESTAMP NOT NULL,
	event_type VARCHAR NOT NULL,
	event_value TEXT NOT NULL,

	CONSTRAINT bonussystem_events_pkey PRIMARY KEY(id)
);
