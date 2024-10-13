CREATE TABLE IF NOT EXISTS dds.fct_product_sales (
	id SERIAL NOT NULL,
	product_id INT NOT NULL,
	order_id INT NOT NULL,
	"count" INT NOT NULL DEFAULT 0,
	price NUMERIC(14,2) NOT NULL DEFAULT 0,
	total_sum NUMERIC(14,2) NOT NULL DEFAULT 0,
	bonus_payment NUMERIC(14,2) NOT NULL DEFAULT 0,
	bonus_grant NUMERIC(14,2) NOT NULL DEFAULT 0,

    CONSTRAINT fct_product_sales_pkey PRIMARY KEY(id),
    CONSTRAINT fct_product_sales_count_check CHECK("count" >= 0),
    CONSTRAINT fct_product_sales_price_check CHECK(price >= (0)::NUMERIC),
    CONSTRAINT fct_product_sales_total_sum_check CHECK(total_sum >= (0)::NUMERIC),
    CONSTRAINT fct_product_sales_bonus_payment_check CHECK(bonus_payment >= (0)::NUMERIC),
    CONSTRAINT fct_product_sales_bonus_grant_check CHECK(bonus_grant >= (0)::NUMERIC),
    CONSTRAINT fct_product_sales_product_id_fkey FOREIGN KEY(product_id) REFERENCES dds.dm_products,
    CONSTRAINT fct_product_sales_order_id_fkey FOREIGN KEY(order_id) REFERENCES dds.dm_orders,
    CONSTRAINT fct_product_sales_product_id_order_id_uindex UNIQUE(product_id, order_id)
);
