from datetime import datetime
from logging import Logger
from typing import List

import psycopg
from bson.objectid import ObjectId
from pydantic import BaseModel, Extra, Field
from lib.connect import PgConnect, HttpConnect
from lib.crud import HttpReader, PgSaver
from lib.etl_settings_repository import EtlSettingsRepository
from lib.utils import CustomSession, json2str
from requests.adapters import HTTPAdapter, Retry


class DeliveryObj(BaseModel, extra=Extra.allow, json_dumps=json2str):
    delivery_id: str
    delivery_ts: datetime

    def __lt__(self, other: 'DeliveryObj'):
        return (self.delivery_ts, self.delivery_id) < (other.delivery_ts, other.delivery_id)


class DeliverySettings(BaseModel, allow_population_by_field_name=True):
    last_loaded_ts: datetime = Field(alias='delivery_ts')
    last_loaded_oid: str = Field(alias='delivery_id', default=str(ObjectId('0' * 24)))


class DeliveriesToStgWorkflow(HttpReader, PgSaver):

    def list_deliveries(self, source: CustomSession, delivery_settings: DeliverySettings, limit: int) -> List[DeliveryObj]:
        # Добавляем повторное подключение, тк возникает ошибка из-за множества запросов при постраничном чтении.
        retries = Retry(total=5, backoff_factor=0.1)
        source.mount('https://', HTTPAdapter(max_retries=retries))

        deliveries = []
        page_size = 50
        offset = 0
        while offset < limit:

            batch = super().list(
                conn=source,
                model=DeliveryObj,
                method='/deliveries',
                params={
                    'sort_field': 'date',
                    'sort_direction': 'asc',
                    'from': delivery_settings.last_loaded_ts.strftime("%Y-%m-%d %H:%M:%S"),
                    'offset': offset,
                    'limit': page_size
                }
            )

            if not batch:
                break

            deliveries += batch
            offset += len(batch)
        
        return deliveries[:limit]


    def insert_delivery(self, target: psycopg.Connection, delivery: DeliveryObj) -> None:
        super().insert(
            conn=target,
            query="""
                INSERT INTO
                    stg.deliverysystem_deliveries(delivery_id, delivery_ts, delivery_value)
                VALUES
                    (%(delivery_id)s, %(delivery_ts)s, %(delivery_value)s)
                ON CONFLICT
                    (delivery_id)
                DO NOTHING
            """,
            params={
                'delivery_id': delivery.delivery_id,
                'delivery_ts': delivery.delivery_ts,
                'delivery_value': delivery.json()
            }
        )


class DeliveriesLoader:
    WF_KEY = 'deliverysystem_deliveries_origin_to_stg_workflow'
    BATCH_LIMIT = 5000

    def __init__(self, http_origin: HttpConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.http_origin = http_origin
        self.pg_dest = pg_dest
        self.workflow = DeliveriesToStgWorkflow()
        self.settings_repository = EtlSettingsRepository(schema='stg')
        self.log = log

    def run(self, start_date: datetime):
        # Открываем соединения.
        with self.http_origin.connection() as source, self.pg_dest.connection() as target:

            # Прочитываем состояние загрузки.
            wf_setting = self.settings_repository.get_setting(target, self.WF_KEY)
            if not wf_setting.workflow_settings:
                wf_setting.workflow_settings['delivery_ts'] = start_date

            delivery_settings = DeliverySettings(**wf_setting.workflow_settings)
            self.log.info(f'Starting to load deliveries from last checkpoint: {delivery_settings}')

            # Вычитываем очередную пачку объектов.
            load_queue = self.workflow.list_deliveries(source, delivery_settings, self.BATCH_LIMIT)
            self.log.info(f'Found {len(load_queue)} deliveries to load.')

            # Если нет объектов, выходим из процесса.
            if not load_queue:
                self.log.info('Quitting.')
                return

            # Сохраняем объекты в базу dwh.
            for delivery in load_queue:
                self.workflow.insert_delivery(target, delivery)

            # Сохраняем прогресс в базу dwh.
            last_loaded_delivery = max(load_queue)
            delivery_settings = DeliverySettings(**last_loaded_delivery.dict())
            self.settings_repository.save_setting(target, wf_setting.workflow_key, delivery_settings)
            self.log.info(f'Load finished on {delivery_settings}')
