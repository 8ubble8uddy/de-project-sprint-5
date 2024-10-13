from datetime import datetime
from logging import Logger
from typing import List, Optional

from bson.objectid import ObjectId
from psycopg import Connection
from pydantic import BaseModel, Field
from lib.connect import PgConnect
from lib.crud import PgReader, PgSaver
from lib.etl_settings_repository import EtlSettingsRepository


class RestaurantObj(BaseModel):
    object_id: Optional[str]
    name: str
    update_ts: datetime

    def __lt__(self, other: 'RestaurantObj'):
        return (self.update_ts, self.object_id) < (other.update_ts, other.object_id)


class RestaurantSettings(BaseModel, allow_population_by_field_name=True):
    last_loaded_ts: datetime = Field(alias='update_ts', default=datetime.min)
    last_loaded_oid: str = Field(alias='object_id', default=str(ObjectId('0' * 24)))


class RestaurantsToStgWorkflow(PgReader, PgSaver):

    def list_restaurants(self, source: Connection, rest_settings: RestaurantSettings, limit: int) -> List[RestaurantObj]:
        restaurants = self.list(
            conn=source,
            model=RestaurantObj,
            # Совмещаем данные ресторанов из двух подсистем, чтобы добиться согласованности данных
            query="""
                SELECT
                    dr.object_id,
                    os.object_value::JSON ->> 'name' AS name,
                    os.update_ts
                FROM
                    stg.ordersystem_restaurants os
                LEFT JOIN
                    stg.deliverysystem_restaurants dr ON dr.object_id = os.object_id
                WHERE
                    os.update_ts > %(ts_threshold)s OR
                    (os.update_ts = %(ts_threshold)s AND os.object_id > %(oid_threshold)s)
                ORDER BY 
                    os.update_ts, os.object_id
                LIMIT
                    %(limit)s;
            """,
            params={
                'ts_threshold': rest_settings.last_loaded_ts,
                'oid_threshold': rest_settings.last_loaded_oid,
                'limit': limit
            }
        )

        # В упорядоченной по дате выборке, оставляем данные до первого отсутствия ресторана в одной из подсистем.
        for idx, restaurant in enumerate(restaurants):
            if not restaurant.object_id:
                restaurants = restaurants[:idx]
                break

        return restaurants

    def insert_restaurant(self, target: Connection, restaurant: RestaurantObj) -> None:
        self.insert(
            conn=target,
            # При вставке ресторана, обновляем (если есть) последнюю запись ресторана, в случае реально новых данных.
            query="""
                WITH update_restaurant_active_to AS (
                    UPDATE
                        dds.dm_restaurants r
                    SET
                        active_to = %(active_from)s
                    FROM
                        (SELECT MAX(id) AS id FROM dds.dm_restaurants WHERE restaurant_id = %(restaurant_id)s) latest
                    WHERE
                        r.id = latest.id AND
                        r.restaurant_name != %(restaurant_name)s
                )
                INSERT INTO
                    dds.dm_restaurants(restaurant_id, restaurant_name, active_from, active_to)
                VALUES
                    (%(restaurant_id)s, %(restaurant_name)s, %(active_from)s, %(active_to)s)
                ON CONFLICT
                    (restaurant_id, active_to)
                DO NOTHING
            """,
            params={
                'restaurant_id': restaurant.object_id,
                'restaurant_name': restaurant.name,
                'active_from': restaurant.update_ts,
                'active_to': datetime(2099, 12, 31)
            }
        )


class RestaurantsLoader:
    WF_KEY = 'restaurants_stg_to_dds_workflow'
    BATCH_LIMIT = 2

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_origin = pg_origin
        self.pg_dest = pg_dest
        self.workflow = RestaurantsToStgWorkflow()
        self.settings_repository = EtlSettingsRepository(schema='dds')
        self.log = log

    def run(self):
        # Открываем соединения.
        with self.pg_origin.connection() as source, self.pg_dest.connection() as target:

            # Прочитываем состояние загрузки.
            wf_setting = self.settings_repository.get_setting(target, self.WF_KEY)
            rest_settings = RestaurantSettings(**wf_setting.workflow_settings)
            self.log.info(f'Starting to load restaurants from last checkpoint: {rest_settings}')

            # Вычитываем очередную пачку объектов.
            load_queue = self.workflow.list_restaurants(source, rest_settings, self.BATCH_LIMIT)
            self.log.info(f'Found {len(load_queue)} restaurants to load.')

            # Если нет объектов, выходим из процесса.
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for restaurant in load_queue:
                self.workflow.insert_restaurant(target, restaurant)

            # Сохраняем прогресс в базу dwh.
            last_loaded_restaurant = max(load_queue)
            rest_settings = RestaurantSettings(**last_loaded_restaurant.dict())
            self.settings_repository.save_setting(target, wf_setting.workflow_key, rest_settings)
            self.log.info(f'Load finished on {rest_settings}')
