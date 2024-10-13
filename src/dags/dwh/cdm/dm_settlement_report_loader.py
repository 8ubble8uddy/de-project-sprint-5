from datetime import date
from logging import Logger
from typing import List

from psycopg import Connection
from pydantic import BaseModel, Field
from lib.connect import PgConnect
from lib.crud import PgReader, PgSaver
from lib.etl_settings_repository import EtlSettingsRepository


class RestaurantReportObj(BaseModel):
    restaurant_id: str
    restaurant_name: str
    date: date
    orders_count: int
    orders_total_sum: float
    bonus_payment_sum: float
    bonus_granted_sum: float

    def __lt__(self, other: 'RestaurantReportObj') -> bool:
        return self.date < other.date


class RestaurantReportSettings(BaseModel, allow_population_by_field_name=True):
    last_loaded_date: date = Field(alias='date', default=date.min)


class RestaurantReportsToCdmWorkflow(PgReader, PgSaver):

    def list_reports(self, source: Connection, report_settings: RestaurantReportSettings, offset: int, limit: int) -> List[RestaurantReportObj]:
        reports = super().list(
            conn=source,
            model=RestaurantReportObj,
            query="""
                SELECT
                    dr.restaurant_id,
                    dr.restaurant_name,
                    dt.date,
                    COUNT(DISTINCT "do".id) AS orders_count,
                    SUM(fps.total_sum) AS orders_total_sum,
                    SUM(fps.bonus_payment) AS bonus_payment_sum,
                    SUM(fps.bonus_grant) AS bonus_granted_sum
                FROM 
                    dds.fct_product_sales fps
                INNER JOIN
                    dds.dm_orders "do" ON "do".id = fps.order_id
                INNER JOIN
                    dds.dm_timestamps dt ON dt.id = "do".timestamp_id
                INNER JOIN
                    dds.dm_restaurants dr ON dr.id = "do".restaurant_id
                WHERE
                    "do".order_status = 'CLOSED' AND dt.date >= %(date_threshold)s
                GROUP BY
                    dt.date, dr.restaurant_id, dr.restaurant_name
                ORDER BY
                    dt.date
                OFFSET
                    %(offset)s
                LIMIT
                    %(limit)s;
            """,
            params={
                'date_threshold': report_settings.last_loaded_date,
                'offset': offset,
                'limit': limit
            }
        )
        return reports

    def insert_report(self, target: Connection, report: RestaurantReportObj) -> None:
        super().insert(
            conn=target,
            query="""
                INSERT INTO
                    cdm.dm_settlement_report(
                        restaurant_id,
                        restaurant_name,
                        settlement_date,
                        orders_count,
                        orders_total_sum,
                        orders_bonus_payment_sum,
                        orders_bonus_granted_sum,
                        order_processing_fee,
                        restaurant_reward_sum)
                VALUES
                    (%(restaurant_id)s,
                    %(restaurant_name)s,
                    %(settlement_date)s,
                    %(orders_count)s,
                    %(orders_total_sum)s,
                    %(orders_bonus_payment_sum)s,
                    %(orders_bonus_granted_sum)s,
                    %(order_processing_fee)s,
                    %(restaurant_reward_sum)s)
                ON CONFLICT
                    (settlement_date, restaurant_id)
                DO UPDATE SET
                    orders_count = EXCLUDED.orders_count,
                    orders_total_sum = EXCLUDED.orders_total_sum,
                    orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
                    orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
                    order_processing_fee = EXCLUDED.order_processing_fee,
                    restaurant_reward_sum = EXCLUDED.restaurant_reward_sum
            """,
            params={
                'restaurant_id': report.restaurant_id, 
                'restaurant_name': report.restaurant_name,
                'settlement_date': report.date,
                'orders_count': report.orders_count,
                'orders_total_sum': report.orders_total_sum,
                'orders_bonus_payment_sum': report.bonus_payment_sum,
                'orders_bonus_granted_sum': report.bonus_granted_sum,
                'order_processing_fee': report.orders_total_sum * 0.25,
                'restaurant_reward_sum': report.orders_total_sum - report.orders_total_sum * 0.25 - report.bonus_payment_sum
            }
        )


class SettlementReportLoader:
    WF_KEY = 'settlement_report_dds_to_cdm_workflow'
    BATCH_LIMIT = 100

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_origin = pg_origin
        self.pg_dest = pg_dest
        self.workflow = RestaurantReportsToCdmWorkflow()
        self.settings_repository = EtlSettingsRepository(schema='cdm')
        self.log = log

    def run(self):
        # Открываем соединения.
        with self.pg_origin.connection() as source, self.pg_dest.connection() as target:

            # Прочитываем состояние загрузки.
            wf_setting = self.settings_repository.get_setting(target, self.WF_KEY)
            report_settings = RestaurantReportSettings(**wf_setting.workflow_settings)
            self.log.info(f'Starting to load settlement report from last checkpoint: {report_settings}')

            # Запускаем цикл до полной выгрузки.
            offset = 0
            last_loaded_report = None
            while True:

                # Вычитываем очередную пачку объектов.
                load_queue = self.workflow.list_reports(source, report_settings, offset, self.BATCH_LIMIT)
                self.log.info(f'Found {len(load_queue)} restaurant reports to load.')

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
                self.log.info(f'Processed {offset} rows while syncing restaurant reports.')

            # Сохраняем прогресс в базу dwh.
            if last_loaded_report:
                report_settings = RestaurantReportSettings(**last_loaded_report.dict())
                self.settings_repository.save_setting(target, wf_setting.workflow_key, report_settings)
                self.log.info(f'Load finished on {report_settings}')
