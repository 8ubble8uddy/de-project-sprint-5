from datetime import datetime
from logging import Logger
from typing import List, Union

import psycopg
from bson.objectid import ObjectId
from pydantic import BaseModel, Field, Extra, validator
from pymongo import mongo_client as pymongo
from lib.connect import PgConnect, MongoConnect
from lib.crud import MongoReader, PgSaver
from lib.etl_settings_repository import EtlSettingsRepository
from lib.utils import json2str


class OrderObj(BaseModel, extra=Extra.allow, arbitrary_types_allowed=True, json_dumps=json2str):
    object_id: ObjectId = Field(alias='_id')
    update_ts: datetime

    def __lt__(self, other: 'OrderObj'):
        return (self.update_ts, self.object_id) < (other.update_ts, other.object_id)


class OrderSettings(BaseModel, allow_population_by_field_name=True, arbitrary_types_allowed=True):
    last_loaded_ts: datetime = Field(alias='update_ts')
    last_loaded_oid: ObjectId = Field(alias='object_id', default=ObjectId('0' * 24))

    @validator('last_loaded_oid', pre=True)
    def serialize_object_id(cls, oid: Union[str, ObjectId]) -> ObjectId:
        return ObjectId(oid)


class OrdersToStgWorkflow(MongoReader, PgSaver):
    
    def list_orders(self, source: pymongo.MongoClient, order_settings: OrderSettings, limit: int) -> List[OrderObj]:
        restaurants = super().list(
            conn=source,
            collection='orders',
            model=OrderObj,
            filter={
                '$or': [
                    {
                        'update_ts': {'$gt': order_settings.last_loaded_ts}
                    },
                    {
                        '$and': [
                            {
                                'update_ts': {'$eq': order_settings.last_loaded_ts}
                            },
                            {
                                '_id': {'$gt': order_settings.last_loaded_oid}
                            }
                        ]
                    }
                ]
            },
            sort=[('update_ts', 1), ('_id', 1)],
            limit=limit
        )
        return restaurants

    def insert_order(self, target: psycopg.Connection, order: OrderObj) -> None:
        super().insert(
            conn=target,
            query="""
                INSERT INTO
                    stg.ordersystem_orders (object_id, object_value, update_ts)
                VALUES
                    (%(id)s, %(val)s, %(update_ts)s)
                ON CONFLICT
                    (object_id)
                DO UPDATE SET
                    object_value = EXCLUDED.object_value,
                    update_ts = EXCLUDED.update_ts;
            """,
            params={
                "id": str(order.object_id),
                "val": order.json(by_alias=True),
                "update_ts": order.update_ts
            }
        )


class OrdersLoader:
    WF_KEY = 'ordersystem_orders_origin_to_stg_workflow'
    BATCH_LIMIT = 5000

    def __init__(self, mongo_origin: MongoConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.mongo_origin = mongo_origin
        self.pg_dest = pg_dest
        self.workflow = OrdersToStgWorkflow()
        self.settings_repository = EtlSettingsRepository(schema='stg')
        self.log = log

    def run(self, start_date: datetime):
        # Открываем соединения.
        with self.mongo_origin.connection() as source, self.pg_dest.connection() as target:

            # Прочитываем состояние загрузки.
            wf_setting = self.settings_repository.get_setting(target, self.WF_KEY)
            if not wf_setting.workflow_settings:
                wf_setting.workflow_settings['update_ts'] = start_date

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
            self.log.info(f'Load finished on {order_settings}')
