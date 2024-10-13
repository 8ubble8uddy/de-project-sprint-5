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


class RestaurantObj(BaseModel, extra=Extra.allow, arbitrary_types_allowed=True, json_dumps=json2str):
    object_id: ObjectId = Field(alias='_id')
    update_ts: datetime

    def __lt__(self, other: 'RestaurantObj'):
        return (self.update_ts, self.object_id) < (other.update_ts, other.object_id)


class RestaurantSettings(BaseModel, allow_population_by_field_name=True, arbitrary_types_allowed=True):
    last_loaded_ts: datetime = Field(alias='update_ts', default=datetime.min)
    last_loaded_oid: ObjectId = Field(alias='object_id', default=ObjectId('0' * 24))

    @validator('last_loaded_oid', pre=True)
    def serialize_object_id(cls, oid: Union[str, ObjectId]) -> ObjectId:
        return ObjectId(oid)


class RestaurantsToStgWorkflow(MongoReader, PgSaver):
    
    def list_restaurants(self, source: pymongo.MongoClient, rest_settings: RestaurantSettings, limit: int) -> List[RestaurantObj]:
        restaurants = super().list(
            conn=source,
            collection='restaurants',
            model=RestaurantObj,
            filter={
                '$or': [
                    {
                        'update_ts': {'$gt': rest_settings.last_loaded_ts}
                    },
                    {
                        '$and': [
                            {
                                'update_ts': {'$eq': rest_settings.last_loaded_ts}
                            },
                            {
                                '_id': {'$gt': rest_settings.last_loaded_oid}
                            }
                        ]
                    }
                ]
            },
            sort=[('update_ts', 1), ('_id', 1)],
            limit=limit
        )
        return restaurants

    def insert_restaurant(self, target: psycopg.Connection, restaurant: RestaurantObj) -> None:
        super().insert(
            conn=target,
            query="""
                INSERT INTO
                    stg.ordersystem_restaurants (object_id, object_value, update_ts)
                VALUES
                    (%(id)s, %(val)s, %(update_ts)s)
                ON CONFLICT
                    (object_id)
                DO UPDATE SET
                    object_value = EXCLUDED.object_value,
                    update_ts = EXCLUDED.update_ts;
            """,
            params={
                "id": str(restaurant.object_id),
                "val": restaurant.json(by_alias=True),
                "update_ts": restaurant.update_ts
            }
        )


class RestaurantsLoader:
    WF_KEY = 'ordersystem_restaurants_origin_to_stg_workflow'
    BATCH_LIMIT = 2

    def __init__(self, mongo_origin: MongoConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.mongo_origin = mongo_origin
        self.pg_dest = pg_dest
        self.workflow = RestaurantsToStgWorkflow()
        self.settings_repository = EtlSettingsRepository(schema='stg')
        self.log = log

    def run(self):
        # Открываем соединения.
        with self.mongo_origin.connection() as source, self.pg_dest.connection() as target:

            # Прочитываем состояние загрузки.
            wf_setting = self.settings_repository.get_setting(target, self.WF_KEY)
            rest_settings = RestaurantSettings(**wf_setting.workflow_settings)
            self.log.info(f'Starting to load restaurants from last checkpoint: {rest_settings}')

            # Вычитываем очередную пачку объектов.
            load_queue = self.workflow.list_restaurants(source, rest_settings, self.BATCH_LIMIT)
            self.log.info(f'Found {len(load_queue)} restaurants to load.')

            # Если нет объектов, выходим из процесса.
            if not load_queue:
                self.log.info('Quitting.')
                return

            # Сохраняем объекты в базу dwh.
            for restaurant in load_queue:
                self.workflow.insert_restaurant(target, restaurant)

            # Сохраняем прогресс в базу dwh.
            last_loaded_restaurant = max(load_queue)
            rest_settings = RestaurantSettings(**last_loaded_restaurant.dict())
            self.settings_repository.save_setting(target, wf_setting.workflow_key, rest_settings)
            self.log.info(f'Load finished on {rest_settings}')
