from logging import Logger
from typing import List, Optional

from psycopg import Connection
from pydantic import BaseModel, Field
from lib.connect import PgConnect
from lib.crud import PgReader, PgSaver
from lib.etl_settings_repository import EtlSettingsRepository


class DeliveryObj(BaseModel):
    id: int
    address: str
    rate: int
    tip_sum: float
    sum: float
    order_fk: Optional[str]
    courier_fk: Optional[str]

    def __lt__(self, other: 'DeliveryObj'):
        return self.id < other.id


class DeliverySettings(BaseModel, allow_population_by_field_name=True):
    last_loaded_id: int = Field(alias='id', default=-1)


class DeliveriesToDdsWorkflow(PgReader, PgSaver):

    def list_deliveries(self, source: Connection, delivery_settings: DeliverySettings, limit: int) -> List[DeliveryObj]:
        deliveries = super().list(
            conn=source,
            model=DeliveryObj,
            # Совмещаем данные продаж из stg-слоя и связанных сущностей dds-слоя, для получения внешних ключей
            query="""
                SELECT
                    sd.id,
                    delivery.address,
                    delivery.rate,
                    delivery.tip_sum,
                    delivery.sum,
                    MAX("do".id) AS order_fk,
                    MAX(dc.id) AS courier_fk
                FROM
                    stg.deliverysystem_deliveries sd
                CROSS JOIN
                    json_to_record(sd.delivery_value::JSON) AS delivery(
                        order_id VARCHAR,
                        courier_id VARCHAR,
                        address VARCHAR,
                        rate INT,
                        tip_sum NUMERIC(14, 2),
                        "sum" NUMERIC(14, 2)
                    )
                LEFT JOIN
                    dds.dm_orders "do" ON "do".order_key = delivery.order_id
                LEFT JOIN
                    dds.dm_couriers dc ON dc.courier_id = delivery.courier_id
                WHERE
                    sd.id > %(id_threshold)s
                GROUP BY
                    sd.id, delivery.address, delivery.rate, delivery.tip_sum, delivery.sum
                ORDER BY
                    sd.id
                LIMIT
                    %(limit)s;
            """,
            params={
                'id_threshold': delivery_settings.last_loaded_id,
                'limit': limit
            }
        )

        # В упорядоченной по дате выборке, оставляем данные до первого отсутствия внешнего ключа какой-либо сущности.
        for idx, delivery in enumerate(deliveries):
            if not all([delivery.order_fk, delivery.courier_fk]):
                deliveries = deliveries[:idx]
                break

        return deliveries

    def insert_delivery(self, target: Connection, delivery: DeliveryObj) -> None:
        super().insert(
            conn=target,
            query="""
                INSERT INTO
                    dds.fct_deliveries(courier_id, order_id, address, rate, tip_sum, sum)
                VALUES
                    (%(courier_id)s, %(order_id)s, %(address)s, %(rate)s, %(tip_sum)s, %(sum)s)
                ON CONFLICT
                    (courier_id, order_id)
                DO NOTHING
            """,
            params={
                'courier_id': delivery.courier_fk,
                'order_id': delivery.order_fk,
                'address': delivery.address, 
                'rate': delivery.rate,
                'tip_sum': delivery.tip_sum,
                'sum': delivery.sum
            }
        )


class DeliveriesLoader:
    WF_KEY = 'courier_deliveries_stg_to_dds_workflow'
    BATCH_LIMIT = 5000

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_origin = pg_origin
        self.pg_dest = pg_dest
        self.workflow = DeliveriesToDdsWorkflow()
        self.settings_repository = EtlSettingsRepository(schema='dds')
        self.log = log

    def run(self):
        # Открываем соединения.
        with self.pg_origin.connection() as source, self.pg_dest.connection() as target:

            # Прочитываем состояние загрузки.
            wf_setting = self.settings_repository.get_setting(target, self.WF_KEY)
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
