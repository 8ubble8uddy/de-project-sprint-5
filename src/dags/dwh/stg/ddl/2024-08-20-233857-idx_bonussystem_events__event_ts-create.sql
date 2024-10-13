CREATE INDEX IF NOT EXISTS idx_bonussystem_events__event_ts ON stg.bonussystem_events USING BTREE(event_ts);
