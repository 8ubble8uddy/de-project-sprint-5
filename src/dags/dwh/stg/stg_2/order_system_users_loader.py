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


class UserObj(BaseModel, extra=Extra.allow, arbitrary_types_allowed=True, json_dumps=json2str):
    object_id: ObjectId = Field(alias='_id')
    update_ts: datetime

    def __lt__(self, other: 'UserObj'):
        return (self.update_ts, self.object_id) < (other.update_ts, other.object_id)


class UserSettings(BaseModel, allow_population_by_field_name=True, arbitrary_types_allowed=True):
    last_loaded_ts: datetime = Field(alias='update_ts', default=datetime.min)
    last_loaded_oid: ObjectId = Field(alias='object_id', default=ObjectId('0' * 24))

    @validator('last_loaded_oid', pre=True)
    def serialize_object_id(cls, oid: Union[str, ObjectId]) -> ObjectId:
        return ObjectId(oid)


class UsersToStgWorkflow(MongoReader, PgSaver):
    
    def list_users(self, source: pymongo.MongoClient, user_settings: UserSettings, limit: int) -> List[UserObj]:
        restaurants = super().list(
            conn=source,
            collection='users',
            model=UserObj,
            filter={
                '$or': [
                    {
                        'update_ts': {'$gt': user_settings.last_loaded_ts}
                    },
                    {
                        '$and': [
                            {
                                'update_ts': {'$eq': user_settings.last_loaded_ts}
                            },
                            {
                                '_id': {'$gt': user_settings.last_loaded_oid}
                            }
                        ]
                    }
                ]
            },
            sort=[('update_ts', 1), ('_id', 1)],
            limit=limit
        )
        return restaurants

    def insert_user(self, target: psycopg.Connection, user: UserObj) -> None:
        super().insert(
            conn=target,
            query="""
                INSERT INTO
                    stg.ordersystem_users (object_id, object_value, update_ts)
                VALUES
                    (%(id)s, %(val)s, %(update_ts)s)
                ON CONFLICT
                    (object_id)
                DO UPDATE SET
                    object_value = EXCLUDED.object_value,
                    update_ts = EXCLUDED.update_ts;
            """,
            params={
                "id": str(user.object_id),
                "val": user.json(by_alias=True),
                "update_ts": user.update_ts
            }
        )


class UsersLoader:
    WF_KEY = 'ordersystem_users_origin_to_stg_workflow'
    BATCH_LIMIT = 50

    def __init__(self, mongo_origin: MongoConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.mongo_origin = mongo_origin
        self.pg_dest = pg_dest
        self.workflow = UsersToStgWorkflow()
        self.settings_repository = EtlSettingsRepository(schema='stg')
        self.log = log

    def run(self):
        # Открываем соединения.
        with self.mongo_origin.connection() as source, self.pg_dest.connection() as target:

            # Прочитываем состояние загрузки.
            wf_setting = self.settings_repository.get_setting(target, self.WF_KEY)
            user_settings = UserSettings(**wf_setting.workflow_settings)
            self.log.info(f'Starting to load users from last checkpoint: {user_settings}')

            # Вычитываем очередную пачку объектов.
            load_queue = self.workflow.list_users(source, user_settings, self.BATCH_LIMIT)
            self.log.info(f'Found {len(load_queue)} users to load.')

            # Если нет объектов, выходим из процесса.
            if not load_queue:
                self.log.info('Quitting.')
                return

            # Сохраняем объекты в базу dwh.
            for user in load_queue:
                self.workflow.insert_user(target, user)

            # Сохраняем прогресс в базу dwh.
            last_loaded_user = max(load_queue)
            user_settings = UserSettings(**last_loaded_user.dict())
            self.settings_repository.save_setting(target, wf_setting.workflow_key, user_settings)
            self.log.info(f'Load finished on {user_settings}')
