from datetime import datetime
from logging import Logger
from typing import List, Optional

from bson.objectid import ObjectId
from psycopg import Connection
from pydantic import BaseModel, Field
from lib.connect import PgConnect
from lib.crud import PgReader, PgSaver
from lib.etl_settings_repository import EtlSettingsRepository


class UserObj(BaseModel):
    object_id: Optional[str] = Field(alias='order_user_id')
    name: str
    login: str
    update_ts: datetime

    def __lt__(self, other: 'UserObj'):
        return (self.update_ts, self.object_id) < (other.update_ts, other.object_id)


class UserSettings(BaseModel, allow_population_by_field_name=True):
    last_loaded_ts: datetime = Field(alias='update_ts', default=datetime.min)
    last_loaded_oid: str = Field(alias='object_id', default=str(ObjectId('0' * 24)))


class UsersToStgWorkflow(PgReader, PgSaver):

    def list_users(self, source: Connection, user_settings: UserSettings, limit: int) -> List[UserObj]:
        users = self.list(
            conn=source,
            model=UserObj,
            # Совмещаем данные пользователей из двух подсистем, чтобы добиться согласованности данных
            query="""
                SELECT
                    bu.order_user_id,
                    ou.object_value::JSON ->> 'name' AS name,
                    ou.object_value::JSON ->> 'login' AS "login",
                    ou.update_ts
                FROM
                    stg.ordersystem_users ou
                LEFT JOIN
                    stg.bonussystem_users bu ON bu.order_user_id = ou.object_id
                WHERE
                    ou.update_ts > %(ts_threshold)s OR
                    (ou.update_ts = %(ts_threshold)s AND ou.object_id > %(oid_threshold)s)
                ORDER BY
                    ou.update_ts, ou.object_id
                LIMIT
                    %(limit)s;
            """,
            params={
                'ts_threshold': user_settings.last_loaded_ts,
                'oid_threshold': user_settings.last_loaded_oid,
                'limit': limit
            }
        )

        # В упорядоченной по дате выборке, оставляем данные до первого отсутствия пользователя в одной из подсистем
        for idx, user in enumerate(users):
            if not user.object_id:
                users = users[:idx]
                break

        return users

    def insert_user(self, target: Connection, user: UserObj) -> None:
        self.insert(
            conn=target,
            query="""
                INSERT INTO
                    dds.dm_users(user_id, user_name, user_login)
                VALUES
                    (%(user_id)s, %(user_name)s, %(user_login)s)
                ON CONFLICT
                    (user_id)
                DO UPDATE SET
                    user_name = EXCLUDED.user_name,
                    user_login = EXCLUDED.user_login;
            """,
            params={
                'user_id': user.object_id,
                'user_name': user.name,
                'user_login': user.login
            }
        )


class UsersLoader:
    WF_KEY = 'users_stg_to_dds_workflow'
    BATCH_LIMIT = 50

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_origin = pg_origin
        self.pg_dest = pg_dest
        self.workflow = UsersToStgWorkflow()
        self.settings_repository = EtlSettingsRepository(schema='dds')
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
            self.log.info(f'Found {len(load_queue)} users to load.')

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
