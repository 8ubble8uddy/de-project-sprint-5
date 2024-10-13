from datetime import datetime
from logging import Logger
from typing import List

import psycopg
from pydantic import BaseModel, Field
from lib.connect import PgConnect
from lib.crud import PgReader, PgSaver
from lib.etl_settings_repository import EtlSettingsRepository


class EventObj(BaseModel):
    id: int
    event_ts: datetime
    event_type: str
    event_value: str

    def __lt__(self, other: 'EventObj'):
        return self.id < other.id


class EventSettings(BaseModel, allow_population_by_field_name=True):
    last_loaded_ts: datetime = Field(alias='event_ts')
    last_loaded_id: int = Field(alias='id', default=-1)


class EventsToStgWorkflow(PgReader, PgSaver):

    def list_events(self, source: psycopg.Connection, event_settings: EventSettings, limit: int) -> List[EventObj]:
        events = super().list(
            conn=source,
            model=EventObj,
            query="""
                SELECT
                    id, event_ts, event_type, event_value
                FROM
                    outbox
                WHERE
                    event_ts > %(ts_threshold)s OR
                    (event_ts = %(ts_threshold)s AND id > %(id_threshold)s)
                ORDER BY
                    event_ts, id
                LIMIT
                    %(limit)s;
            """,
            params={
                'ts_threshold': event_settings.last_loaded_ts,
                'id_threshold': event_settings.last_loaded_id,
                'limit': limit
            }
        )
        return events

    def insert_event(self, target: psycopg.Connection, event: EventObj) -> None:
        super().insert(
            conn=target,
            query="""
                INSERT INTO
                    stg.bonussystem_events(id, event_ts, event_type, event_value)
                VALUES
                    (%(id)s, %(event_ts)s, %(event_type)s, %(event_value)s)
                ON CONFLICT
                    (id)
                DO UPDATE SET
                    event_ts = EXCLUDED.event_ts,
                    event_type = EXCLUDED.event_type,
                    event_value = EXCLUDED.event_value;
            """,
            params={
                'id': event.id,
                'event_ts': event.event_ts,
                'event_type': event.event_type,
                'event_value': event.event_value
            }
        )


class EventsLoader:
    WF_KEY = 'bonussystem_events_origin_to_stg_workflow'
    BATCH_LIMIT = 10000

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_origin = pg_origin
        self.pg_dest = pg_dest
        self.workflow = EventsToStgWorkflow()
        self.settings_repository = EtlSettingsRepository(schema='stg')
        self.log = log

    def run(self, start_date: datetime):
        # Открываем соединения.
        with self.pg_origin.connection() as source, self.pg_dest.connection() as target:

            # Прочитываем состояние загрузки.
            wf_setting = self.settings_repository.get_setting(target, self.WF_KEY)
            if not wf_setting.workflow_settings:
                wf_setting.workflow_settings['event_ts'] = start_date

            event_settings = EventSettings(**wf_setting.workflow_settings)
            self.log.info(f'Starting to load events from last checkpoint: {event_settings}')

            # Вычитываем очередную пачку объектов.
            load_queue = self.workflow.list_events(source, event_settings, self.BATCH_LIMIT)
            self.log.info(f'Found {len(load_queue)} events to load.')

            # Если нет объектов, выходим из процесса.
            if not load_queue:
                self.log.info('Quitting.')
                return

            # Сохраняем объекты в базу dwh.
            for event in load_queue:
                self.workflow.insert_event(target, event)

            # Сохраняем прогресс в базу dwh.
            last_loaded_event = max(load_queue)
            event_settings = EventSettings(**last_loaded_event.dict())
            self.settings_repository.save_setting(target, wf_setting.workflow_key, event_settings)
            self.log.info(f'Load finished on {event_settings}')
