CREATE TABLE IF NOT EXISTS dds.dm_timestamps (
	id SERIAL NOT NULL,
	ts TIMESTAMP NOT NULL,
	"year" SMALLINT NOT NULL,
	"month" SMALLINT NOT NULL,
	"day" SMALLINT NOT NULL,
	"time" TIME NOT NULL,
	"date" DATE NOT NULL,

    CONSTRAINT dm_timestamps_pkey PRIMARY KEY(id),
    CONSTRAINT dm_timestamps_year_check CHECK(("year" >= 2022) AND ("year" < 2500)),
    CONSTRAINT dm_timestamps_month_check CHECK(("month" >= 1) AND ("month" <= 12)),
    CONSTRAINT dm_timestamps_day_check CHECK(("day" >= 1) AND ("day" <= 31)),
	CONSTRAINT dm_timestamps_ts_uindex UNIQUE(ts)
);
