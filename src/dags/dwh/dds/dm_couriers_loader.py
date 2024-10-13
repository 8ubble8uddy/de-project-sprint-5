from logging import Logger
from typing import List

from psycopg import Connection
from pydantic import BaseModel, Field
from lib.connect import PgConnect
from lib.crud import PgReader, PgSaver
from lib.etl_settings_repository import EtlSettingsRepository


class CourierObj(BaseModel):
    id: int
    object_id: str
    name: str

    def __lt__(self, other: 'CourierObj'):
        return self.id < other.id


class CourierSettings(BaseModel, allow_population_by_field_name=True):
    last_loaded_id: int = Field(alias='id', default=-1)


class CouriersToStgWorkflow(PgReader, PgSaver):

    def list_couriers(self, source: Connection, courier_settings: CourierSettings, limit: int) -> List[CourierObj]:
        couriers = super().list(
            conn=source,
            model=CourierObj,
            query="""
                SELECT
                    id, object_id, name
                FROM
                    stg.deliverysystem_couriers
                WHERE
                    id > %(id_threshold)s
                ORDER BY
                    id
                LIMIT
                    %(limit)s;
            """,
            params={
                'id_threshold': courier_settings.last_loaded_id,
                'limit': limit
            }
        )
        return couriers

    def insert_courier(self, target: Connection, courier: CourierObj) -> None:
        super().insert(
            conn=target,
            query="""
                INSERT INTO
                    dds.dm_couriers(courier_id, courier_name)
                VALUES
                    (%(courier_id)s, %(courier_name)s)
                ON CONFLICT
                    (courier_id)
                DO UPDATE SET
                    courier_name = EXCLUDED.courier_name
            """,
            params={
                'courier_id': courier.object_id,
                'courier_name': courier.name
            }
        )


class CouriersLoader:
    WF_KEY = 'couriers_stg_to_dds_workflow'
    BATCH_LIMIT = 50

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_origin = pg_origin
        self.pg_dest = pg_dest
        self.workflow = CouriersToStgWorkflow()
        self.settings_repository = EtlSettingsRepository(schema='dds')
        self.log = log

    def run(self):
        # Открываем соединения.
        with self.pg_origin.connection() as source, self.pg_dest.connection() as target:

            # Прочитываем состояние загрузки. ы
            wf_setting = self.settings_repository.get_setting(target, self.WF_KEY)
            courier_settings = CourierSettings(**wf_setting.workflow_settings)
            self.log.info(f'Starting to load couriers from last checkpoint: {courier_settings}')

            # Вычитываем очередную пачку объектов.
            load_queue = self.workflow.list_couriers(source, courier_settings, self.BATCH_LIMIT)
            self.log.info(f'Found {len(load_queue)} couriers to load.')

            # Если нет объектов, выходим из процесса.
            if not load_queue:
                self.log.info('Quitting.')
                return

            # Сохраняем объекты в базу dwh.
            for courier in load_queue:
                self.workflow.insert_courier(target, courier)

            # Сохраняем прогресс в базу dwh.
            last_loaded_courier = max(load_queue)
            courier_settings = CourierSettings(**last_loaded_courier.dict())
            self.settings_repository.save_setting(target, wf_setting.workflow_key, courier_settings)
            self.log.info(f'Load finished on {courier_settings}')
