from datetime import datetime
from logging import Logger
from typing import List

from psycopg import Connection
from pydantic import BaseModel, Field
from lib.connect import PgConnect
from lib.crud import PgReader, PgSaver
from lib.etl_settings_repository import EtlSettingsRepository


class OrderObj(BaseModel):
    date: datetime

    def __lt__(self, other: 'OrderObj'):
        return self.date < other.date


class OrderSettings(BaseModel, allow_population_by_field_name=True):
    last_loaded_ts: datetime = Field(alias='date', default=datetime.min)


class TimestampsToStgWorkflow(PgReader, PgSaver):

    def list_orders(self, source: Connection, order_settings: OrderSettings, limit: int) -> List[OrderObj]:
        orders = super().list(
            conn=source,
            model=OrderObj,
            query="""
                SELECT
                    "order".date
                FROM
                    stg.ordersystem_orders oo
                CROSS JOIN
                    json_to_record(oo.object_value::JSON) AS "order"(
                        final_status VARCHAR,
                        date TIMESTAMP
                    )
                WHERE
                    "order".date > %(ts_threshold)s AND
                    "order".final_status IN ('CLOSED', 'CANCELLED')
                ORDER BY
                    "order".date
                LIMIT
                    %(limit)s;
            """,
            params={
                'ts_threshold': order_settings.last_loaded_ts,
                'limit': limit
            }
        )
        return orders

    def insert_order_ts(self, target: Connection, order_ts: datetime) -> None:
        self.insert(
            conn=target,
            query=(
            """
                INSERT INTO
                    dds.dm_timestamps(ts, year, month, day, time, date)
                VALUES
                    (%(ts)s, %(year)s, %(month)s, %(day)s, %(time)s, %(date)s)
                ON CONFLICT
                    (ts)
                DO NOTHING
            """),
            params={
                    'ts': order_ts,
                    'year': order_ts.year,
                    'month': order_ts.month,
                    'day': order_ts.day,
                    'time': order_ts.time(),
                    'date': order_ts.date()
            }
        )


class TimestampsLoader:
    WF_KEY = 'timestamps_stg_to_dds_workflow'
    BATCH_LIMIT = 5000

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_origin = pg_origin
        self.pg_dest = pg_dest
        self.workflow = TimestampsToStgWorkflow()
        self.settings_repository = EtlSettingsRepository(schema='dds')
        self.log = log

    def run(self):
        # Открываем соединения.
        with self.pg_origin.connection() as source, self.pg_dest.connection() as target:

            # Прочитываем состояние загрузки.
            wf_setting = self.settings_repository.get_setting(target, self.WF_KEY)
            order_settings = OrderSettings(**wf_setting.workflow_settings)
            self.log.info(f'Starting to load timestamps from last checkpoint: {order_settings}')

            # Вычитываем очередную пачку объектов.
            load_queue = self.workflow.list_orders(source, order_settings, self.BATCH_LIMIT)
            self.log.info(f'Found {len(load_queue)} timestamps to load.')

            # Если нет объектов, выходим из процесса.
            if not load_queue:
                self.log.info('Quitting.')
                return

            # Сохраняем объекты в базу dwh.
            for order in load_queue:
                self.workflow.insert_order_ts(target, order.date)

            # Сохраняем прогресс в базу dwh.
            last_loaded_order = max(load_queue)
            order_settings = OrderSettings(**last_loaded_order.dict())
            self.settings_repository.save_setting(target, wf_setting.workflow_key, order_settings)
            self.log.info(f'Load finished on {order_settings}')
