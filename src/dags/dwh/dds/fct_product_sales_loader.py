from datetime import datetime
from logging import Logger
from typing import List, Optional

from bson.objectid import ObjectId
from psycopg import Connection
from pydantic import BaseModel, Field
from lib.connect import PgConnect
from lib.crud import PgReader, PgSaver
from lib.etl_settings_repository import EtlSettingsRepository


class ProductSaleObj(BaseModel):
    product_id: str
    price: float
    quantity: int
    bonus_payment: float
    bonus_grant: float
    event_ts: datetime
    order_fk: Optional[str]
    product_fk: Optional[str]

    def __lt__(self, other: 'ProductSaleObj'):
        return (self.event_ts, self.product_id) < (other.event_ts, other.product_id)


class ProductSaleSettings(BaseModel, allow_population_by_field_name=True):
    last_loaded_ts: datetime = Field(alias='event_ts', default=datetime.min)
    last_loaded_product_id: str = Field(alias='product_id', default=str(ObjectId('0' * 24)))


class ProductSalesToDdsWorkflow(PgReader, PgSaver):

    def list_sales(self, source: Connection, sale_settings: ProductSaleSettings, limit: int) -> List[ProductSaleObj]:
        product_sales = super().list(
            conn=source,
            model=ProductSaleObj,
            # Совмещаем данные продаж из stg-слоя и связанных сущностей dds-слоя, для получения внешних ключей
            query="""
                SELECT
                    product.product_id,
                    product.price,
                    product.quantity,
                    product.bonus_payment,
                    product.bonus_grant,
                    be.event_ts,
                    MAX("do".id) AS order_fk,
                    MAX(dp.id) AS product_fk
                FROM
                    stg.bonussystem_events be
                CROSS JOIN
                    json_to_recordset(be.event_value::JSON -> 'product_payments') AS product(
                        product_id VARCHAR,
                        price NUMERIC(14,2),
                        quantity INT,
                        bonus_payment NUMERIC(14,2),
                        bonus_grant NUMERIC(14,2)
                    )
                LEFT JOIN
                    dds.dm_orders "do" ON "do".order_key = be.event_value::JSON ->> 'order_id'
                LEFT JOIN
                    dds.dm_products dp ON dp.product_id = product.product_id
                WHERE
                    be.event_ts > %(ts_threshold)s OR
                    (be.event_ts = %(ts_threshold)s AND product.product_id > %(oid_threshold)s)
                GROUP BY
                    be.event_ts, product.product_id, product.price, product.quantity, product.bonus_payment, product.bonus_grant
                ORDER BY
                    be.event_ts, product.product_id
                LIMIT
                    %(limit)s;
            """,
            params={
                'ts_threshold': sale_settings.last_loaded_ts,
                'oid_threshold': sale_settings.last_loaded_product_id,
                'limit': limit
            }
        )

        # В упорядоченной по дате выборке, оставляем данные до первого отсутствия внешнего ключа какой-либо сущности.
        for idx, product_sale in enumerate(product_sales):
            if not all([product_sale.order_fk, product_sale.product_fk]):
                product_sales = product_sales[:idx]
                break

        return product_sales

    def insert_sale(self, target: Connection, product_sale: ProductSaleObj) -> None:
        super().insert(
            conn=target,
            query="""
                INSERT INTO
                    dds.fct_product_sales(product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant)
                VALUES
                    (%(product_id)s, %(order_id)s, %(count)s, %(price)s, %(total_sum)s, %(bonus_payment)s, %(bonus_grant)s)
                ON CONFLICT
                    (product_id, order_id)
                DO NOTHING
            """,
            params={
                'order_id': product_sale.order_fk,
                'product_id': product_sale.product_fk,
                'count': product_sale.quantity, 
                'price': product_sale.price,
                'total_sum': product_sale.quantity * product_sale.price,
                'bonus_payment': product_sale.bonus_payment,
                'bonus_grant': product_sale.bonus_grant
            }
        )


class ProductSalesLoader:
    WF_KEY = 'product_sales_stg_to_dds_workflow'
    BATCH_LIMIT = 10000

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_origin = pg_origin
        self.pg_dest = pg_dest
        self.workflow = ProductSalesToDdsWorkflow()
        self.settings_repository = EtlSettingsRepository(schema='dds')
        self.log = log

    def run(self):
        # Открываем соединения.
        with self.pg_origin.connection() as source, self.pg_dest.connection() as target:

            # Прочитываем состояние загрузки.
            wf_setting = self.settings_repository.get_setting(target, self.WF_KEY)
            sale_settings = ProductSaleSettings(**wf_setting.workflow_settings)
            self.log.info(f'Starting to load product sales from last checkpoint: {sale_settings}')

            # Вычитываем очередную пачку объектов.
            load_queue = self.workflow.list_sales(source, sale_settings, self.BATCH_LIMIT)
            self.log.info(f'Found {len(load_queue)} product sales to load.')

            # Если нет объектов, выходим из процесса.
            if not load_queue:
                self.log.info('Quitting.')
                return

            # Сохраняем объекты в базу dwh.
            for product_sale in load_queue:
                self.workflow.insert_sale(target, product_sale)

            # Сохраняем прогресс в базу dwh.
            last_loaded_sale = max(load_queue)
            sale_settings = ProductSaleSettings(**last_loaded_sale.dict())
            self.settings_repository.save_setting(target, wf_setting.workflow_key, sale_settings)
            self.log.info(f'Load finished on {sale_settings}')
