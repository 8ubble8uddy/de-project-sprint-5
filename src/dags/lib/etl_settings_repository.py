from typing import Any, Dict

from psycopg import Connection
from pydantic import BaseModel, Field
from lib.crud import PgReader, PgSaver
from lib.utils import json2str


class EtlSetting(BaseModel):
    id: int
    workflow_key: str
    workflow_settings: Dict = Field(default_factory=dict)


class EtlSettingsRepository(PgReader, PgSaver):
    def __init__(self, schema: str) -> None:
        self.schema = schema

    def get_setting(self, conn: Connection, wf_key: str) -> EtlSetting:
        query = (
            """
                SELECT id, workflow_key, workflow_settings
                FROM {schema}.srv_wf_settings
                WHERE workflow_key = %(etl_key)s;
            """
        ).format(schema=self.schema)

        etl_setting = self.retrieve(conn, query, EtlSetting, {'etl_key': wf_key})

        return etl_setting or EtlSetting(id=0, workflow_key=wf_key)

    def save_setting(self, сonn: Connection, wf_key: str, wf_settings: Any) -> None:
        query = (
            """
                INSERT INTO {schema}.srv_wf_settings (workflow_key, workflow_settings)
                VALUES (%(etl_key)s, %(etl_setting)s)
                ON CONFLICT (workflow_key) DO UPDATE
                SET workflow_settings = EXCLUDED.workflow_settings;
            """
        ).format(schema=self.schema)

        wf_settings_json = json2str(wf_settings)

        self.insert(сonn, query, {'etl_key': wf_key, 'etl_setting': wf_settings_json})
