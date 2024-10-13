from datetime import datetime
from logging import Logger
from typing import List

from psycopg import Connection
from pydantic import BaseModel, Field
from lib.connect import PgConnect
from lib.crud import PgReader, PgSaver
from lib.etl_settings_repository import EtlSettingsRepository


class CourierReportObj(BaseModel):
    courier_id: str
    courier_name: str
    year: int
    month: int
    orders_count: int
    orders_total_sum: float
    rate_avg: float
    courier_order_sum: float
    courier_tips_sum: float

    def __lt__(self, other: 'CourierReportObj') -> bool:
        return (self.year, self.month) < (other.year, other.month)


class CourierReportSettings(BaseModel, allow_population_by_field_name=True):
    last_loaded_year: int = Field(alias='year', default=datetime.min.year)
    last_loaded_month: int = Field(alias='month', default=datetime.min.month)


class CourierReportsToCdmWorkflow(PgReader, PgSaver):

    def list_reports(self, source: Connection, report_settings: CourierReportSettings, offset: int, limit: int) -> List[CourierReportObj]:
        reports = super().list(
            conn=source,
            model=CourierReportObj,
            query="""
                SELECT
                    dc.courier_id,
                    dc.courier_name,
                    dt.year,
                    dt.month,
                    COUNT("do".id) AS orders_count,
                    SUM(fd.sum) AS orders_total_sum,
                    AVG(fd.rate) AS rate_avg,
                    CASE
                        WHEN AVG(fd.rate) < 4 THEN SUM(GREATEST(fd.sum * 0.05 / 100, 100))
                        WHEN AVG(fd.rate) < 4.5 THEN SUM(GREATEST(fd.sum * 0.07 / 100, 150))
                        WHEN AVG(fd.rate) < 4.9 THEN SUM(GREATEST(fd.sum * 0.08 / 100, 175))
                        ELSE SUM(GREATEST(fd.sum * 0.10 / 100, 200))
                    END AS courier_order_sum,
                    SUM(fd.tip_sum) AS courier_tips_sum
                FROM 
                    dds.fct_deliveries fd
                INNER JOIN
                    dds.dm_couriers dc ON dc.id = fd.courier_id
                INNER JOIN
                    dds.dm_orders "do" ON "do".id = fd.order_id
                INNER JOIN
                    dds.dm_timestamps dt ON dt.id = "do".timestamp_id
                WHERE
                    dt.year >= %(year_threshold)s AND dt.month >= %(month_threshold)s
                GROUP BY
                    dt.year, dt.month, dc.courier_id, dc.courier_name
                ORDER BY
                    dt.year, dt.month
                OFFSET
                    %(offset)s
                LIMIT
                    %(limit)s;
            """,
            params={
                'year_threshold': report_settings.last_loaded_year,
                'month_threshold': report_settings.last_loaded_month,
                'offset': offset,
                'limit': limit
            }
        )
        return reports

    def insert_report(self, target: Connection, report: CourierReportObj) -> None:
        super().insert(
            conn=target,
            query="""
                INSERT INTO
                    cdm.dm_courier_ledger(
                        courier_id,
                        courier_name,
                        settlement_year,
                        settlement_month,
                        orders_count,
                        orders_total_sum,
                        rate_avg,
                        order_processing_fee,
                        courier_order_sum,
                        courier_tips_sum,
                        courier_reward_sum)
                VALUES
                    (%(courier_id)s,
                    %(courier_name)s,
                    %(settlement_year)s,
                    %(settlement_month)s,
                    %(orders_count)s,
                    %(orders_total_sum)s,
                    %(rate_avg)s,
                    %(order_processing_fee)s,
                    %(courier_order_sum)s,
                    %(courier_tips_sum)s,
                    %(courier_reward_sum)s)
                ON CONFLICT
                    (settlement_year, settlement_month, courier_id)
                DO UPDATE SET
                    orders_count = EXCLUDED.orders_count,
                    orders_total_sum = EXCLUDED.orders_total_sum,
                    rate_avg = EXCLUDED.rate_avg,
                    order_processing_fee = EXCLUDED.order_processing_fee,
                    courier_order_sum = EXCLUDED.courier_order_sum,
                    courier_tips_sum = EXCLUDED.courier_tips_sum,
                    courier_reward_sum = EXCLUDED.courier_reward_sum
            """,
            params={
                'courier_id': report.courier_id, 
                'courier_name': report.courier_name,
                'settlement_year': report.year,
                'settlement_month': report.month,
                'orders_count': report.orders_count,
                'orders_total_sum': report.orders_total_sum,
                'rate_avg': report.rate_avg,
                'order_processing_fee': report.orders_total_sum * 0.25,
                'courier_order_sum': report.courier_order_sum,
                'courier_tips_sum': report.courier_tips_sum,
                'courier_reward_sum': report.orders_total_sum + report.courier_tips_sum * 0.95
            }
        )


class CourierLedgerLoader:
    WF_KEY = 'courier_ledger_dds_to_cdm_workflow'
    BATCH_LIMIT = 100

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_origin = pg_origin
        self.pg_dest = pg_dest
        self.workflow = CourierReportsToCdmWorkflow()
        self.settings_repository = EtlSettingsRepository(schema='cdm')
        self.log = log

    def run(self):
        # Открываем соединения.
        with self.pg_origin.connection() as source, self.pg_dest.connection() as target:

            # Прочитываем состояние загрузки.
            wf_setting = self.settings_repository.get_setting(target, self.WF_KEY)
            report_settings = CourierReportSettings(**wf_setting.workflow_settings)
            self.log.info(f'Starting to load courier ledger from last checkpoint: {report_settings}')

            # Запускаем цикл до полной выгрузки.
            offset = 0
            last_loaded_report = None
            while True:

                # Вычитываем очередную пачку объектов.
                load_queue = self.workflow.list_reports(source, report_settings, offset, self.BATCH_LIMIT)
                self.log.info(f'Found {len(load_queue)} courier reports to load.')

                # Если нет объектов, выходим из цикла.
                if not load_queue:
                    self.log.info('Quitting.')
                    break

                # Сохраняем объекты в базу dwh.
                for report in load_queue:
                    self.workflow.insert_report(target, report)

                # Сдвигаем параметр смещения offset.
                offset += len(load_queue)
                last_loaded_report = max(load_queue)
                self.log.info(f'Processed {offset} rows while syncing courier reports.')

            # Сохраняем прогресс в базу dwh.
            if last_loaded_report:
                report_settings = CourierReportSettings(**last_loaded_report.dict())
                self.settings_repository.save_setting(target, wf_setting.workflow_key, report_settings)
                self.log.info(f'Load finished on {report_settings}')
