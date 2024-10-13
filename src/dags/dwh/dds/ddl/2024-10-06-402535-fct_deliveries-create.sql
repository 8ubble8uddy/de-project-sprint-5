CREATE TABLE IF NOT EXISTS dds.fct_deliveries (
	id SERIAL NOT NULL,
	order_id INT NOT NULL,
    courier_id INT NOT NULL,
    "address" TEXT NOT NULL,
    rate SMALLINT NOT NULL,
    tip_sum NUMERIC(14,2) NOT NULL DEFAULT 0,
    "sum" NUMERIC(14,2) NOT NULL DEFAULT 0,

    CONSTRAINT fct_deliveries_pkey PRIMARY KEY(id),
    CONSTRAINT fct_deliveries_rate_check CHECK(rate BETWEEN 1 AND 5),
    CONSTRAINT fct_deliveries_tip_sum_check CHECK(tip_sum >= (0)::NUMERIC),
    CONSTRAINT fct_deliveries_sum_check CHECK("sum" >= (0)::NUMERIC),
    CONSTRAINT fct_deliveries_order_id_fkey FOREIGN KEY(order_id) REFERENCES dds.dm_orders,
    CONSTRAINT fct_deliveries_courier_id_fkey FOREIGN KEY(courier_id) REFERENCES dds.dm_couriers,
    CONSTRAINT fct_deliveries_order_id_courier_id_uindex UNIQUE(courier_id, order_id)
);
