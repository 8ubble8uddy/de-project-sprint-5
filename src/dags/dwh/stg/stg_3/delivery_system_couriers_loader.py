from logging import Logger
from typing import List

import psycopg
from bson.objectid import ObjectId
from pydantic import BaseModel, Field
from lib.connect import PgConnect, HttpConnect
from lib.crud import HttpReader, PgSaver
from lib.etl_settings_repository import EtlSettingsRepository
from lib.utils import CustomSession


class CourierObj(BaseModel):
    object_id: str = Field(alias='_id')
    name: str

    def __lt__(self, other: 'CourierObj'):
        return self.object_id < other.object_id


class CourierSettings(BaseModel, allow_population_by_field_name=True):
    last_loaded_oid: str = Field(alias='object_id', default=str(ObjectId('0' * 24)))
    offset: int = 0


class CouriersToStgWorkflow(HttpReader, PgSaver):

    def list_couriers(self, source: CustomSession, courier_settings: CourierSettings, limit: int) -> List[CourierObj]: 
        couriers = []
        page_size = 50
        offset = 0
        while offset < limit:

            batch = super().list(
                conn=source,
                model=CourierObj,
                method='/couriers',
                params={
                    'sort_field': 'id',
                    'sort_direction': 'asc',
                    'offset': courier_settings.offset + offset,
                    'limit': page_size
                }
            )

            if not batch:
                break

            couriers += batch
            offset += len(batch)
        
        return couriers[:limit]


    def insert_courier(self, target: psycopg.Connection, courier: CourierObj) -> None:
        super().insert(
            conn=target,
            query="""
                INSERT INTO
                    stg.deliverysystem_couriers(object_id, name)
                VALUES
                    (%(object_id)s, %(name)s)
                ON CONFLICT
                    (object_id)
                DO UPDATE SET
                    name = EXCLUDED.name;
            """,
            params={
                'object_id': courier.object_id,
                'name': courier.name
            }
        )


class CouriersLoader:
    WF_KEY = 'deliverysystem_couriers_origin_to_stg_workflow'
    BATCH_LIMIT = 50

    def __init__(self, http_origin: HttpConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.http_origin = http_origin
        self.pg_dest = pg_dest
        self.workflow = CouriersToStgWorkflow()
        self.settings_repository = EtlSettingsRepository(schema='stg')
        self.log = log

    def run(self):
        # Открываем соединения.
        with self.http_origin.connection() as source, self.pg_dest.connection() as target:

            # Прочитываем состояние загрузки.
            wf_setting = self.settings_repository.get_setting(target, self.WF_KEY)
            courier_settings = CourierSettings(**wf_setting.workflow_settings)
            self.log.info(f'Starting to load couriers from last checkpoint: {courier_settings}')

            # Вычитываем очередную пачку объектов.
            load_queue = self.workflow.list_couriers(source, courier_settings, self.BATCH_LIMIT)
            self.log.info(f'Found {len(load_queue)} couriers to load.')

            # Если нет объектов, выходим из процесса.
            if not load_queue:
                self.log.info('Quitting')
                return

            # Сохраняем объекты в базу dwh.
            for courier in load_queue:
                self.workflow.insert_courier(target, courier)

            # Сохраняем прогресс в базу dwh.
            last_loaded, offset = max(load_queue), courier_settings.offset + len(load_queue)
            courier_settings = CourierSettings(**last_loaded.dict(), offset=offset)
            self.settings_repository.save_setting(target, wf_setting.workflow_key, courier_settings)
            self.log.info(f'Load finished on {courier_settings}')
