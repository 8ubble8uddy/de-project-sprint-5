CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger (
	id INT GENERATED ALWAYS AS IDENTITY,
    courier_id VARCHAR(2048) NOT NULL,
    courier_name VARCHAR(2048) NOT NULL,
    settlement_year SMALLINT NOT NULL,
    settlement_month SMALLINT NOT NULL,
	orders_count INT NOT NULL DEFAULT 0,
	orders_total_sum NUMERIC(14,2) NOT NULL DEFAULT 0,
    rate_avg NUMERIC(19,5) NOT NULL DEFAULT 0,
    order_processing_fee NUMERIC(14,2) NOT NULL DEFAULT 0,
    courier_order_sum NUMERIC(14,2) NOT NULL DEFAULT 0,
    courier_tips_sum NUMERIC(14,2) NOT NULL DEFAULT 0,
    courier_reward_sum NUMERIC(14,2) NOT NULL DEFAULT 0,

    CONSTRAINT dm_courier_ledger_pkey PRIMARY KEY(id),
    CONSTRAINT dm_courier_ledger_settlement_year_check CHECK(settlement_year >= 2022 AND settlement_year < 2500),
    CONSTRAINT dm_courier_ledger_settlement_month_check CHECK(settlement_month >= 1 AND settlement_month <= 12),
    CONSTRAINT dm_courier_ledger_rate_avg_check CHECK(rate_avg >= (1)::NUMERIC),
    CONSTRAINT dm_courier_ledger_order_processing_fee_check CHECK(order_processing_fee >= (0)::NUMERIC),
    CONSTRAINT dm_courier_ledger_courier_order_sum_check CHECK(courier_order_sum >= (0)::NUMERIC),
    CONSTRAINT dm_courier_ledger_courier_tips_sum_check CHECK(courier_tips_sum >= (0)::NUMERIC),
    CONSTRAINT dm_courier_ledger_courier_reward_sum_check CHECK(courier_reward_sum >= (0)::NUMERIC),
    CONSTRAINT dm_courier_ledger_courier_id_settlement_year_settlement_month_uindex UNIQUE(courier_id, settlement_year, settlement_month)
);
