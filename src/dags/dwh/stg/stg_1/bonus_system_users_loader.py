from logging import Logger
from typing import List

import psycopg
from pydantic import BaseModel, Field
from lib.connect import PgConnect
from lib.crud import PgReader, PgSaver
from lib.etl_settings_repository import EtlSettingsRepository


class UserObj(BaseModel):
    id: int
    order_user_id: str

    def __lt__(self, other: 'UserObj'):
        return self.id < other.id


class UserSettings(BaseModel, allow_population_by_field_name=True):
    last_loaded_id: int = Field(alias='id', default=-1)


class UsersToStgWorkflow(PgReader, PgSaver):

    def list_users(self, source: psycopg.Connection, user_settings: UserSettings, limit: int) -> List[UserObj]:
        users = super().list(
            conn=source,
            model=UserObj,
            query="""
                SELECT
                    id, order_user_id
                FROM
                    users
                WHERE
                    id > %(id_threshold)s
                ORDER BY
                    id
                LIMIT
                    %(limit)s;
            """,
            params={
                'id_threshold': user_settings.last_loaded_id,
                'limit': limit
            }
        )
        return users

    def insert_user(self, target: psycopg.Connection, user: UserObj) -> None:
        super().insert(
            conn=target,
            query="""
                INSERT INTO
                    stg.bonussystem_users(id, order_user_id)
                VALUES
                    (%(id)s, %(order_user_id)s)
                ON CONFLICT
                    (id)
                DO UPDATE SET
                    order_user_id = EXCLUDED.order_user_id;
            """,
            params={
                'id': user.id,
                'order_user_id': user.order_user_id
            }
        )


class UsersLoader:
    WF_KEY = 'bonussystem_users_origin_to_stg_workflow'
    BATCH_LIMIT = 50

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_origin = pg_origin
        self.pg_dest = pg_dest
        self.workflow = UsersToStgWorkflow()
        self.settings_repository = EtlSettingsRepository(schema='stg')
        self.log = log

    def run(self):
        # Открываем соединения.
        with self.pg_origin.connection() as source, self.pg_dest.connection() as target:

            # Прочитываем состояние загрузки.
            wf_setting = self.settings_repository.get_setting(target, self.WF_KEY)
            user_settings = UserSettings(**wf_setting.workflow_settings)
            self.log.info(f'Starting to load users from last checkpoint: {user_settings}')

            # Вычитываем очередную пачку объектов.
            load_queue = self.workflow.list_users(source, user_settings, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} users to load.")

            # Если нет объектов, выходим из процесса.
            if not load_queue:
                self.log.info('Quitting.')
                return

            # Сохраняем объекты в базу dwh.
            for user in load_queue:
                self.workflow.insert_user(target, user)

            # Сохраняем прогресс в базу dwh.
            last_loaded_user = max(load_queue)
            user_settings = UserSettings(**last_loaded_user.dict())
            self.settings_repository.save_setting(target, wf_setting.workflow_key, user_settings)
            self.log.info(f'Load finished on {user_settings}')
