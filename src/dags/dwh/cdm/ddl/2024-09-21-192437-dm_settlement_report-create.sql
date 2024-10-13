CREATE TABLE IF NOT EXISTS cdm.dm_settlement_report (
	id INT GENERATED ALWAYS AS IDENTITY,
	restaurant_id VARCHAR(2048) NOT NULL,
	restaurant_name VARCHAR(2048) NOT NULL,
	settlement_date DATE NOT NULL,
	orders_count INT NOT NULL DEFAULT 0,
	orders_total_sum NUMERIC(14,2) NOT NULL DEFAULT 0,
	orders_bonus_payment_sum NUMERIC(14,2) NOT NULL DEFAULT 0,
	orders_bonus_granted_sum NUMERIC(14,2) NOT NULL DEFAULT 0,
	order_processing_fee NUMERIC(14,2) NOT NULL DEFAULT 0,
	restaurant_reward_sum NUMERIC(14,2) NOT NULL DEFAULT 0,

    CONSTRAINT dm_settlement_report_pkey PRIMARY KEY(id),
    CONSTRAINT dm_settlement_report_settlement_date_check CHECK(settlement_date >= '2022-01-01' AND settlement_date < '2500-01-01'),
    CONSTRAINT dm_settlement_report_orders_count_check CHECK(orders_count >= 0),
    CONSTRAINT dm_settlement_report_orders_total_sum_check CHECK(orders_total_sum >= (0)::numeric),
    CONSTRAINT dm_settlement_report_orders_bonus_payment_sum_check CHECK(orders_bonus_payment_sum >= (0)::numeric),
    CONSTRAINT dm_settlement_report_orders_bonus_granted_sum_check CHECK(orders_bonus_granted_sum >= (0)::numeric),
    CONSTRAINT dm_settlement_report_order_processing_fee_check CHECK(order_processing_fee >= (0)::numeric),
    CONSTRAINT dm_settlement_report_restaurant_reward_sum_check CHECK(restaurant_reward_sum >= (0)::numeric),
    CONSTRAINT dm_settlement_report_settlement_date_restaurant_id_uindex UNIQUE(settlement_date, restaurant_id)
);
