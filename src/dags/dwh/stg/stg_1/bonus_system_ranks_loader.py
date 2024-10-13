from logging import Logger
from typing import List

import psycopg
from pydantic import BaseModel, Field
from lib.crud import PgReader, PgSaver
from lib.etl_settings_repository import EtlSettingsRepository
from lib.connect import PgConnect


class RankObj(BaseModel):
    id: int
    name: str
    bonus_percent: float
    min_payment_threshold: float

    def __lt__(self, other: 'RankObj'):
        return self.id < other.id


class RankSettings(BaseModel, allow_population_by_field_name=True):
    last_loaded_id: int = Field(alias='id', default=-1)


class RanksToStgWorkrlow(PgReader, PgSaver):

    def list_ranks(self, source: psycopg.Connection, rank_settings: RankSettings, limit: int) -> List[RankObj]:
        ranks = super().list(
            conn=source,
            model=RankObj,
            query=(
            """
                SELECT
                    id, name, bonus_percent, min_payment_threshold
                FROM
                    ranks
                WHERE
                    id > %(id_threshold)s
                ORDER BY
                    id
                LIMIT
                    %(limit)s;
            """),
            params={
                'id_threshold': rank_settings.last_loaded_id,
                'limit': limit
            }
        )
        return ranks

    def insert_rank(self, target: psycopg.Connection, rank: RankObj) -> None:
        super().insert(
            conn=target,
            query=(
            """
                INSERT INTO 
                    stg.bonussystem_ranks(id, name, bonus_percent, min_payment_threshold)
                VALUES
                    (%(id)s, %(name)s, %(bonus_percent)s, %(min_payment_threshold)s)
                ON CONFLICT 
                    (id)
                DO UPDATE SET
                    name = EXCLUDED.name,
                    bonus_percent = EXCLUDED.bonus_percent,
                    min_payment_threshold = EXCLUDED.min_payment_threshold;
            """),
            params={
                'id': rank.id,
                'name': rank.name,
                'bonus_percent': rank.bonus_percent,
                'min_payment_threshold': rank.min_payment_threshold
            }
        )


class RanksLoader:
    WF_KEY = 'bonussystem_ranks_origin_to_stg_workflow'
    BATCH_LIMIT = 2

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_origin = pg_origin
        self.pg_dest = pg_dest
        self.workflow = RanksToStgWorkrlow()
        self.settings_repository = EtlSettingsRepository(schema='stg')
        self.log = log

    def run(self):
        # Открываем соединения.
        with self.pg_origin.connection() as source, self.pg_dest.connection() as target:

            # Прочитываем состояние загрузки.
            wf_setting = self.settings_repository.get_setting(target, self.WF_KEY)
            rank_settings = RankSettings(**wf_setting.workflow_settings)
            self.log.info(f'Starting to load ranks from last checkpoint: {rank_settings}')

            # Вычитываем очередную пачку объектов.
            load_queue = self.workflow.list_ranks(source, rank_settings, self.BATCH_LIMIT)
            self.log.info(f'Found {len(load_queue)} ranks to load.')

            # Если нет объектов, выходим из процесса.
            if not load_queue:
                self.log.info('Quitting.')
                return

            # Сохраняем объекты в базу dwh.
            for rank in load_queue:
                self.workflow.insert_rank(target, rank)

            # Сохраняем прогресс в базу dwh.
            last_loaded_rank = max(load_queue)
            rank_settings = RankSettings(**last_loaded_rank.dict())
            self.settings_repository.save_setting(target, wf_setting.workflow_key, rank_settings)
            self.log.info(f'Load finished on {rank_settings}')

