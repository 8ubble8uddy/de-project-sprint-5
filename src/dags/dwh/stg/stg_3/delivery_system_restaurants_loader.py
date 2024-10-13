from logging import Logger
from typing import List

import psycopg
from bson.objectid import ObjectId
from pydantic import BaseModel, Field
from lib.connect import PgConnect, HttpConnect
from lib.crud import HttpReader, PgSaver
from lib.etl_settings_repository import EtlSettingsRepository
from lib.utils import CustomSession


class RestaurantObj(BaseModel):
    object_id: str = Field(alias='_id')
    name: str

    def __lt__(self, other: 'RestaurantObj'):
        return self.object_id < other.object_id


class RestaurantSettings(BaseModel, allow_population_by_field_name=True):
    last_loaded_oid: str = Field(alias='object_id', default=str(ObjectId('0' * 24)))
    offset: int = 0


class RestaurantsToStgWorkflow(HttpReader, PgSaver):

    def list_restaurants(self, source: CustomSession, rest_settings: RestaurantSettings, limit: int) -> List[RestaurantObj]:
        restaurants = []
        page_size = 50
        offset = 0
        while offset < limit:

            batch = super().list(
                conn=source,
                model=RestaurantObj,
                method='/restaurants',
                params={
                    'sort_field': 'id',
                    'sort_direction': 'asc',
                    'offset': rest_settings.offset + offset,
                    'limit': page_size
                }
            )

            if not batch:
                break

            restaurants += batch
            offset += len(batch)
        
        return restaurants[:limit]

    def insert_restaurant(self, target: psycopg.Connection, restaurant: RestaurantObj) -> None:
        super().insert(
            conn=target,
            query="""
                INSERT INTO
                    stg.deliverysystem_restaurants(object_id, name)
                VALUES
                    (%(object_id)s, %(name)s)
                ON CONFLICT
                    (object_id)
                DO UPDATE SET
                    name = EXCLUDED.name;
            """,
            params={
                "object_id": restaurant.object_id,
                "name": restaurant.name
            }
        )


class RestaurantsLoader:
    WF_KEY = 'deliverysystem_restaurants_origin_to_stg_workflow'
    BATCH_LIMIT = 2

    def __init__(self, http_origin: HttpConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.http_origin = http_origin
        self.pg_dest = pg_dest
        self.workflow = RestaurantsToStgWorkflow()
        self.settings_repository = EtlSettingsRepository(schema='stg')
        self.log = log

    def run(self):
        # Открываем соединения.
        with self.http_origin.connection() as source, self.pg_dest.connection() as target:

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
            last_loaded, offset = max(load_queue), rest_settings.offset + len(load_queue)
            rest_settings = RestaurantSettings(**last_loaded.dict(), offset=offset)
            self.settings_repository.save_setting(target, wf_setting.workflow_key, rest_settings)
            self.log.info(f'Load finished on {rest_settings}')
