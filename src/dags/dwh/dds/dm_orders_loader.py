from datetime import datetime
from logging import Logger
from typing import List, Optional

from bson.objectid import ObjectId
from psycopg import Connection
from pydantic import BaseModel, Field
from lib.connect import PgConnect
from lib.crud import PgReader, PgSaver
from lib.etl_settings_repository import EtlSettingsRepository


class OrderObj(BaseModel):
    object_id: str
    update_ts: datetime
    final_status: str
    user_fk: Optional[int]
    restaurant_fk: Optional[int]
    timestamp_fk: Optional[int]

    def __lt__(self, other: 'OrderObj'):
        return (self.update_ts, self.object_id) < (other.update_ts, other.object_id)


class OrderSettings(BaseModel, allow_population_by_field_name=True):
    last_loaded_ts: datetime = Field(alias='update_ts', default=datetime.min)
    last_loaded_oid: str = Field(alias='object_id', default=str(ObjectId('0' * 24)))


class OrdersToDdsWorkflow(PgReader, PgSaver):

    def list_orders(self, source: Connection, order_settings: OrderSettings, limit: int) -> List[OrderObj]:
        orders = super().list(
            conn=source,
            model=OrderObj,
            # Совмещаем данные заказов из stg-слоя и связанных сущностей dds-слоя, для получения внешних ключей
            query="""
                SELECT
                    oo.object_id,
                    oo.update_ts,
                    "order".final_status,
                    MAX(du.id) AS user_fk,
                    MAX(dr.id) AS restaurant_fk,
                    MAX(dt.id) AS timestamp_fk
                FROM
                    stg.ordersystem_orders oo
                CROSS JOIN
                    json_to_record(oo.object_value::JSON) AS "order"(
                        final_status VARCHAR,
                        "user" JSON,
                        restaurant JSON,
                        "date" TIMESTAMP
                    )
                LEFT JOIN
                    dds.dm_users du ON du.user_id = "order".user ->> 'id'
                LEFT JOIN
                    dds.dm_restaurants dr ON dr.restaurant_id = "order".restaurant ->> 'id'
                LEFT JOIN
                    dds.dm_timestamps dt ON dt.ts = "order".date
                WHERE
                    oo.update_ts > %(ts_threshold)s OR
                    (oo.update_ts = %(ts_threshold)s AND oo.object_id > %(oid_threshold)s)
                GROUP BY
                    oo.update_ts, oo.object_id, "order".final_status
                ORDER BY
                    oo.update_ts, oo.object_id
                LIMIT
                    %(limit)s;
            """,
            params={
                'ts_threshold': order_settings.last_loaded_ts,
                'oid_threshold': order_settings.last_loaded_oid,
                'limit': limit
            }
        )

        # В упорядоченной по дате выборке, оставляем данные до первого отсутствия внешнего ключа какой либо сущности.
        for idx, order in enumerate(orders):
            if not all([order.user_fk, order.restaurant_fk, order.timestamp_fk]):
                orders = orders[:idx]
                break

        return orders

    def insert_order(self, target: Connection, order: OrderObj) -> None:
        super().insert(
            conn=target,
            query="""
                INSERT INTO
                    dds.dm_orders(order_key, order_status, user_id, restaurant_id, timestamp_id)
                VALUES
                    (%(order_key)s, %(order_status)s, %(user_id)s, %(restaurant_id)s, %(timestamp_id)s)
                ON CONFLICT
                    (order_key)
                DO UPDATE SET
                    order_status = EXCLUDED.order_status,
                    user_id = EXCLUDED.user_id,
                    restaurant_id = EXCLUDED.restaurant_id,
                    timestamp_id = EXCLUDED.timestamp_id
            """,
            params={
                'order_key': order.object_id, 
                'order_status': order.final_status,
                'user_id': order.user_fk,
                'restaurant_id': order.restaurant_fk,
                'timestamp_id': order.timestamp_fk
            }
        )


class OrdersLoader:
    WF_KEY = 'orders_stg_to_dds_workflow'
    BATCH_LIMIT = 5000

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_origin = pg_origin
        self.pg_dest = pg_dest
        self.workflow = OrdersToDdsWorkflow()
        self.settings_repository = EtlSettingsRepository(schema='dds')
        self.log = log

    def run(self):
        # Открываем соединения.
        with self.pg_origin.connection() as source, self.pg_dest.connection() as target:

            # Прочитываем состояние загрузки.
            wf_setting = self.settings_repository.get_setting(target, self.WF_KEY)
            order_settings = OrderSettings(**wf_setting.workflow_settings)
            self.log.info(f'Starting to load orders from last checkpoint: {order_settings}')

            # Вычитываем очередную пачку объектов.
            load_queue = self.workflow.list_orders(source, order_settings, self.BATCH_LIMIT)
            self.log.info(f'Found {len(load_queue)} orders to load.')

            # Если нет объектов, выходим из процесса.
            if not load_queue:
                self.log.info('Quitting.')
                return

            # Сохраняем объекты в базу dwh.
            for order in load_queue:
                self.workflow.insert_order(target, order)

            # Сохраняем прогресс в базу dwh.
            last_loaded_order = max(load_queue)
            order_settings = OrderSettings(**last_loaded_order.dict())
            self.settings_repository.save_setting(target, wf_setting.workflow_key, order_settings)
            self.log.info(f'Load finished on {order_settings.last_loaded_ts}')
