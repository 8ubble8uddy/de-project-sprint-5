from datetime import datetime
from logging import Logger
from typing import List

from bson.objectid import ObjectId
from psycopg import Connection
from pydantic import BaseModel, Field
from lib.connect import PgConnect
from lib.crud import PgReader, PgSaver
from lib.etl_settings_repository import EtlSettingsRepository


class ProductObj(BaseModel):
    product_id: str = Field(alias='_id')
    restaurant_id: str
    name: str
    price: float
    update_ts: datetime
    restaurant_fk: int

    def __lt__(self, other: 'ProductObj') -> bool:
        return (self.update_ts, self.restaurant_id, self.product_id) < (other.update_ts, other.restaurant_id, other.product_id)


class ProductSettings(BaseModel, allow_population_by_field_name=True):
    last_loaded_ts: datetime = Field(alias='update_ts', default=datetime.min)
    last_loaded_restaurant_id: str = Field(alias='restaurant_id', default=str(ObjectId('0' * 24)))
    last_loaded_product_id: str = Field(alias='product_id', default=str(ObjectId('0' * 24)))


class ProductsToDdsWorkflow(PgReader, PgSaver):

    def list_products(self, source: Connection, product_settings: ProductSettings, limit: int) -> List[ProductObj]:
        products = super().list(
            conn=source,
            model=ProductObj,
            # Совмещаем данные продуктов из stg-слоя и ресторанов dds-слоя, для получения внешних ключей
            query="""
                SELECT
                    dr.restaurant_id,
                    product._id,
                    product.name,
                    product.price,
                    "or".update_ts,
                    MAX(dr.id) AS restaurant_fk
                FROM
                    stg.ordersystem_restaurants "or"
                CROSS JOIN
                    json_to_recordset("or".object_value::JSON -> 'menu') AS product(
                        _id VARCHAR,
                        name VARCHAR,
                        price NUMERIC(14, 2)
                    )
                INNER JOIN
                    dds.dm_restaurants dr ON dr.restaurant_id = "or".object_id
                WHERE
                    "or".update_ts > %(ts_threshold)s OR
                    ("or".update_ts = %(ts_threshold)s AND
                        (dr.restaurant_id > %(restaurant_id_threshold)s OR
                            (dr.restaurant_id = %(restaurant_id_threshold)s AND product._id > %(product_id_threshold)s)))
                GROUP BY
                    "or".update_ts, dr.restaurant_id, product._id, product.name, product.price
                ORDER BY
                    "or".update_ts, dr.restaurant_id, product._id
                LIMIT
                    %(limit)s;
            """,
            params={
                'ts_threshold': product_settings.last_loaded_ts,
                'restaurant_id_threshold': product_settings.last_loaded_restaurant_id,
                'product_id_threshold': product_settings.last_loaded_product_id,
                'limit': limit
            }
        )
        return products

    def insert_product(self, target: Connection, product: ProductObj) -> None:
        super().insert(
            conn=target,
            # При вставке продукта, обновляем (если есть) последнюю запись продукта, в случае реально новых данных.
            query="""
                WITH update_product_active_to AS (
                    UPDATE 
                        dds.dm_products p
                    SET
                        active_to = %(active_from)s
                    FROM
                        (SELECT MAX(id) AS id FROM dds.dm_products WHERE product_id = %(product_id)s) latest
                    WHERE
                        p.id = latest.id AND
                        (p.product_name != %(product_name)s OR
                        p.product_price != %(product_price)s OR
                        p.restaurant_id != %(restaurant_id)s)
                )
                INSERT INTO
                    dds.dm_products(product_id, product_name, product_price, restaurant_id, active_from, active_to)
                VALUES
                    (%(product_id)s, %(product_name)s, %(product_price)s, %(restaurant_id)s, %(active_from)s, %(active_to)s)
                ON CONFLICT
                    (product_id, active_to)
                DO NOTHING
            """,
            params={
                'product_id': product.product_id, 
                'product_name': product.name,
                'product_price': product.price,
                'restaurant_id': product.restaurant_fk,
                'active_from': product.update_ts,
                'active_to': datetime(2099, 12, 31),
            }
        )


class ProductsLoader:
    WF_KEY = 'products_stg_to_dds_workflow'
    BATCH_LIMIT = 100

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_origin = pg_origin
        self.pg_dest = pg_dest
        self.workflow = ProductsToDdsWorkflow()
        self.settings_repository = EtlSettingsRepository(schema='dds')
        self.log = log

    def run(self):
        # Открываем соединения.
        with self.pg_origin.connection() as source, self.pg_dest.connection() as target:

            # Прочитываем состояние загрузки.
            wf_setting = self.settings_repository.get_setting(target, self.WF_KEY)
            product_settings = ProductSettings(**wf_setting.workflow_settings)
            self.log.info(f'Starting to load products from last checkpoint: {product_settings}')

            # Вычитываем очередную пачку объектов.
            load_queue = self.workflow.list_products(source, product_settings, self.BATCH_LIMIT)
            self.log.info(f'Found {len(load_queue)} products to load.')

            # Если нет объектов, выходим из процесса.
            if not load_queue:
                self.log.info('Quitting.')
                return

            # Сохраняем объекты в базу dwh.
            for product in load_queue:
                self.workflow.insert_product(target, product)

            # Сохраняем прогресс в базу dwh.
            last_loaded_product = max(load_queue)
            product_settings = ProductSettings(**last_loaded_product.dict())
            self.settings_repository.save_setting(target, wf_setting.workflow_key, product_settings)
            self.log.info(f'Load finished on {product_settings}')
